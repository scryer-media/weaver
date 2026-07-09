#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgArguments, PgPool, PgRow};
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqlitePool, SqliteRow};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Postgres, Row, Sqlite, Transaction};
use tokio::sync::Mutex;

use crate::persistence::StateError;

const SQLITE_BUSY_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1000),
];
const SQLITE_BUSY_RETRY_HARD_CAP: Duration = Duration::from_secs(120);
const POSTGRES_TRANSIENT_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(10),
    Duration::from_millis(50),
    Duration::from_millis(150),
    Duration::from_millis(400),
];

pub(crate) type SqlResult<T> = Result<T, StateError>;

#[derive(Clone)]
pub(crate) enum StoreDatastore {
    Sqlite {
        pool: SqlitePool,
        writer_gate: Arc<Mutex<()>>,
    },
    Postgres {
        pool: PgPool,
    },
}

impl StoreDatastore {
    pub(crate) fn read_exec(&self) -> SqlExec<'_, '_> {
        match self {
            Self::Sqlite { pool, .. } => SqlExec::Target(SqlTarget::Sqlite(pool)),
            Self::Postgres { pool } => SqlExec::Target(SqlTarget::Postgres(pool)),
        }
    }

    pub(crate) fn engine(&self) -> SqlEngine {
        match self {
            Self::Sqlite { .. } => SqlEngine::Sqlite,
            Self::Postgres { .. } => SqlEngine::Postgres,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlEngine {
    Sqlite,
    Postgres,
}

impl SqlEngine {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

pub(crate) enum SqlTarget<'a> {
    Sqlite(&'a SqlitePool),
    Postgres(&'a PgPool),
}

pub(crate) enum SqlExec<'tx, 'db> {
    Target(SqlTarget<'db>),
    Tx(&'tx mut SqlTx<'db>),
}

pub(crate) type TxFuture<'a, T> = Pin<Box<dyn Future<Output = SqlResult<T>> + Send + 'a>>;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum SqlArg {
    Text(String),
    OptText(Option<String>),
    I32(i32),
    I64(i64),
    F64(f64),
    OptI32(Option<i32>),
    OptI64(Option<i64>),
    OptF64(Option<f64>),
    Bool(bool),
    OptBool(Option<bool>),
    Timestamp(DateTime<Utc>),
    OptTimestamp(Option<DateTime<Utc>>),
    Json(JsonValue),
    OptJson(Option<JsonValue>),
    Bytes(Vec<u8>),
    OptBytes(Option<Vec<u8>>),
}

#[derive(Clone, Copy, Debug)]
enum PlaceholderDialect {
    Sqlite,
    Postgres,
}

pub(crate) enum SqlRow {
    Sqlite(SqliteRow),
    Postgres(PgRow),
}

pub(crate) enum SqlTx<'db> {
    Sqlite(Transaction<'db, Sqlite>),
    Postgres(Transaction<'db, Postgres>),
}

pub(crate) struct SqlRuntime;

impl SqlRuntime {
    pub(crate) async fn run_serialized_sqlite<T, Op, Fut>(
        datastore: &StoreDatastore,
        op_name: &'static str,
        mut op: Op,
    ) -> SqlResult<T>
    where
        T: Send,
        Op: FnMut(SqlitePool) -> Fut + Send,
        Fut: Future<Output = SqlResult<T>> + Send,
    {
        let StoreDatastore::Sqlite { pool, writer_gate } = datastore else {
            return Err(StateError::Database(format!(
                "operation `{op_name}` requires sqlite datastore"
            )));
        };

        let started = Instant::now();
        let _guard = writer_gate.lock().await;
        let result = run_with_sqlite_busy_retries(op_name, || op(pool.clone())).await;
        crate::runtime::perf_probe::record_sql_op("sqlite", op_name, started.elapsed());
        result
    }

    pub(crate) async fn run_serialized_sqlite_connection<T, Op, Fut>(
        datastore: &StoreDatastore,
        op_name: &'static str,
        op: Op,
    ) -> SqlResult<T>
    where
        Op: FnOnce(sqlx::pool::PoolConnection<Sqlite>) -> Fut,
        Fut: Future<Output = SqlResult<T>>,
    {
        let StoreDatastore::Sqlite { pool, writer_gate } = datastore else {
            return Err(StateError::Database(format!(
                "operation `{op_name}` requires sqlite datastore"
            )));
        };

        let started = Instant::now();
        let _guard = writer_gate.lock().await;
        let conn = pool.acquire().await.map_err(db_err)?;
        let result = op(conn).await;
        crate::runtime::perf_probe::record_sql_op("sqlite", op_name, started.elapsed());
        result
    }

    pub(crate) async fn execute(
        exec: SqlExec<'_, '_>,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<u64> {
        match exec {
            SqlExec::Target(SqlTarget::Sqlite(pool)) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query.execute(pool).await.map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "execute",
                    template,
                    started.elapsed(),
                );
                let result = result?;
                Ok(result.rows_affected())
            }
            SqlExec::Target(SqlTarget::Postgres(pool)) => {
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                // Autocommit write: a bare statement's commit outcome is unknown
                // on a dropped connection, so only retry guaranteed-rolled-back
                // (serialization/deadlock) failures, not connection errors.
                let result = run_with_postgres_retries("execute", false, || async {
                    let started = Instant::now();
                    let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                    let outcome = query.execute(pool).await.map_err(pg_db_err);
                    // Record each attempt's real query time, excluding retry sleeps.
                    crate::runtime::perf_probe::record_sql_statement(
                        "postgres",
                        "execute",
                        template,
                        started.elapsed(),
                    );
                    outcome
                })
                .await?;
                Ok(result.rows_affected())
            }
            SqlExec::Tx(tx) => tx.execute(template, args).await,
        }
    }

    /// Autocommit write that returns a row (e.g. `INSERT ... RETURNING`). Unlike
    /// [`Self::fetch_optional`], this uses the WRITE retry policy (retry mode
    /// `false`): a bare write's commit outcome is unknown on a dropped
    /// connection, so only guaranteed-rolled-back failures are retried, never
    /// connection errors — retrying those would duplicate the inserted row.
    pub(crate) async fn execute_returning(
        exec: SqlExec<'_, '_>,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<Option<SqlRow>> {
        match exec {
            SqlExec::Target(SqlTarget::Sqlite(pool)) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_optional(pool)
                    .await
                    .map(|row| row.map(SqlRow::Sqlite))
                    .map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "execute_returning",
                    template,
                    started.elapsed(),
                );
                result
            }
            SqlExec::Target(SqlTarget::Postgres(pool)) => {
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                // Autocommit write: same outcome-unknown reasoning as `execute`,
                // so only retry guaranteed-rolled-back failures, not connection
                // errors.
                run_with_postgres_retries("execute_returning", false, || async {
                    let started = Instant::now();
                    let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                    let outcome = query
                        .fetch_optional(pool)
                        .await
                        .map(|row| row.map(SqlRow::Postgres))
                        .map_err(pg_db_err);
                    // Record each attempt's real query time, excluding retry sleeps.
                    crate::runtime::perf_probe::record_sql_statement(
                        "postgres",
                        "execute_returning",
                        template,
                        started.elapsed(),
                    );
                    outcome
                })
                .await
            }
            SqlExec::Tx(tx) => tx.fetch_optional(template, args).await,
        }
    }

    pub(crate) async fn fetch_optional(
        exec: SqlExec<'_, '_>,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<Option<SqlRow>> {
        match exec {
            SqlExec::Target(SqlTarget::Sqlite(pool)) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_optional(pool)
                    .await
                    .map(|row| row.map(SqlRow::Sqlite))
                    .map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "fetch_optional",
                    template,
                    started.elapsed(),
                );
                result
            }
            SqlExec::Target(SqlTarget::Postgres(pool)) => {
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                // Read: idempotent, so retry connection errors as well.
                run_with_postgres_retries("fetch_optional", true, || async {
                    let started = Instant::now();
                    let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                    let outcome = query
                        .fetch_optional(pool)
                        .await
                        .map(|row| row.map(SqlRow::Postgres))
                        .map_err(pg_db_err);
                    crate::runtime::perf_probe::record_sql_statement(
                        "postgres",
                        "fetch_optional",
                        template,
                        started.elapsed(),
                    );
                    outcome
                })
                .await
            }
            SqlExec::Tx(tx) => tx.fetch_optional(template, args).await,
        }
    }

    pub(crate) async fn fetch_all(
        exec: SqlExec<'_, '_>,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<Vec<SqlRow>> {
        match exec {
            SqlExec::Target(SqlTarget::Sqlite(pool)) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_all(pool)
                    .await
                    .map(|rows| rows.into_iter().map(SqlRow::Sqlite).collect())
                    .map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "fetch_all",
                    template,
                    started.elapsed(),
                );
                result
            }
            SqlExec::Target(SqlTarget::Postgres(pool)) => {
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                // Read: idempotent, so retry connection errors as well.
                run_with_postgres_retries("fetch_all", true, || async {
                    let started = Instant::now();
                    let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                    let outcome = query
                        .fetch_all(pool)
                        .await
                        .map(|rows| rows.into_iter().map(SqlRow::Postgres).collect())
                        .map_err(pg_db_err);
                    crate::runtime::perf_probe::record_sql_statement(
                        "postgres",
                        "fetch_all",
                        template,
                        started.elapsed(),
                    );
                    outcome
                })
                .await
            }
            SqlExec::Tx(tx) => tx.fetch_all(template, args).await,
        }
    }

    pub(crate) async fn run_in_transaction<T, F>(
        datastore: &StoreDatastore,
        op_name: &'static str,
        op: F,
    ) -> SqlResult<T>
    where
        T: Send,
        F: for<'tx, 'db> Fn(&'tx mut SqlTx<'db>) -> TxFuture<'tx, T> + Send + Sync,
    {
        let engine = datastore.engine().as_str();
        match datastore {
            StoreDatastore::Sqlite { pool, writer_gate } => {
                let started = Instant::now();
                let _guard = writer_gate.lock().await;
                let result = run_with_sqlite_busy_retries(op_name, || {
                    let pool = pool.clone();
                    let op = &op;
                    async move {
                        let mut tx = SqlTx::Sqlite(pool.begin().await.map_err(db_err)?);
                        let result = {
                            let future = op(&mut tx);
                            future.await?
                        };
                        tx.commit().await?;
                        Ok(result)
                    }
                })
                .await;
                crate::runtime::perf_probe::record_sql_op(engine, op_name, started.elapsed());
                result
            }
            StoreDatastore::Postgres { pool } => {
                // Phase-aware retry. A begin/op failure applied nothing durable,
                // so it is retried under the ordinary rules (connection errors +
                // guaranteed-rolled-back class 40). A COMMIT failure is only
                // retried when it is guaranteed rolled back (40001/40P01); a
                // connection-class error at COMMIT leaves the outcome UNKNOWN —
                // the server may have committed before the ack was lost — so
                // re-running would duplicate plain INSERT batches. `op` is `Fn`,
                // so re-invocation before commit is sound.
                run_postgres_transaction_with_retries(op_name, || async {
                    let started = Instant::now();
                    let mut tx =
                        SqlTx::Postgres(pool.begin().await.map_err(|e| {
                            PostgresTxError::new(TxPhase::BeforeCommit, pg_db_err(e))
                        })?);
                    let result = {
                        let future = op(&mut tx);
                        future
                            .await
                            .map_err(|e| PostgresTxError::new(TxPhase::BeforeCommit, e))?
                    };
                    tx.commit()
                        .await
                        .map_err(|e| PostgresTxError::new(TxPhase::Commit, e))?;
                    // Record the successful attempt's real duration only (retry
                    // sleeps and failed attempts are excluded).
                    crate::runtime::perf_probe::record_sql_op(engine, op_name, started.elapsed());
                    Ok(result)
                })
                .await
            }
        }
    }
}

impl<'db> SqlTx<'db> {
    pub(crate) async fn execute(&mut self, template: &str, args: &[SqlArg]) -> SqlResult<u64> {
        match self {
            SqlTx::Sqlite(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query.execute(&mut **tx).await.map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "tx_execute",
                    template,
                    started.elapsed(),
                );
                let result = result?;
                Ok(result.rows_affected())
            }
            SqlTx::Postgres(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query.execute(&mut **tx).await.map_err(pg_db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "postgres",
                    "tx_execute",
                    template,
                    started.elapsed(),
                );
                let result = result?;
                Ok(result.rows_affected())
            }
        }
    }

    pub(crate) async fn fetch_optional(
        &mut self,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<Option<SqlRow>> {
        match self {
            SqlTx::Sqlite(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_optional(&mut **tx)
                    .await
                    .map(|row| row.map(SqlRow::Sqlite))
                    .map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "tx_fetch_optional",
                    template,
                    started.elapsed(),
                );
                result
            }
            SqlTx::Postgres(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_optional(&mut **tx)
                    .await
                    .map(|row| row.map(SqlRow::Postgres))
                    .map_err(pg_db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "postgres",
                    "tx_fetch_optional",
                    template,
                    started.elapsed(),
                );
                result
            }
        }
    }

    pub(crate) async fn fetch_all(
        &mut self,
        template: &str,
        args: &[SqlArg],
    ) -> SqlResult<Vec<SqlRow>> {
        match self {
            SqlTx::Sqlite(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Sqlite, args.len())?;
                let query = bind_sqlite(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_all(&mut **tx)
                    .await
                    .map(|rows| rows.into_iter().map(SqlRow::Sqlite).collect())
                    .map_err(db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "sqlite",
                    "tx_fetch_all",
                    template,
                    started.elapsed(),
                );
                result
            }
            SqlTx::Postgres(tx) => {
                let started = Instant::now();
                let sql = render_sql(template, PlaceholderDialect::Postgres, args.len())?;
                let query = bind_postgres(sqlx::query(AssertSqlSafe(sql.as_str())), args);
                let result = query
                    .fetch_all(&mut **tx)
                    .await
                    .map(|rows| rows.into_iter().map(SqlRow::Postgres).collect())
                    .map_err(pg_db_err);
                crate::runtime::perf_probe::record_sql_statement(
                    "postgres",
                    "tx_fetch_all",
                    template,
                    started.elapsed(),
                );
                result
            }
        }
    }

    pub(crate) async fn commit(self) -> SqlResult<()> {
        match self {
            SqlTx::Sqlite(tx) => tx.commit().await.map_err(db_err),
            SqlTx::Postgres(tx) => tx.commit().await.map_err(pg_db_err),
        }
    }
}

#[allow(dead_code)]
impl SqlRow {
    pub(crate) fn text(&self, column: &str) -> SqlResult<String> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn opt_text(&self, column: &str) -> SqlResult<Option<String>> {
        match self {
            SqlRow::Sqlite(row) => opt_text_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => opt_text_from_pg_row(row, column),
        }
    }

    pub(crate) fn i64(&self, column: &str) -> SqlResult<i64> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => i64_from_pg_row(row, column),
        }
    }

    pub(crate) fn i64_at(&self, column: usize) -> SqlResult<i64> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn i32(&self, column: &str) -> SqlResult<i32> {
        match self {
            SqlRow::Sqlite(row) => i32_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => i32_from_pg_row(row, column),
        }
    }

    pub(crate) fn opt_i64(&self, column: &str) -> SqlResult<Option<i64>> {
        match self {
            SqlRow::Sqlite(row) => opt_i64_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => opt_i64_from_pg_row(row, column),
        }
    }

    pub(crate) fn opt_i32(&self, column: &str) -> SqlResult<Option<i32>> {
        match self {
            SqlRow::Sqlite(row) => opt_i32_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => opt_i32_from_pg_row(row, column),
        }
    }

    pub(crate) fn opt_f64(&self, column: &str) -> SqlResult<Option<f64>> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn bool(&self, column: &str) -> SqlResult<bool> {
        match self {
            SqlRow::Sqlite(row) => bool_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => bool_from_pg_row(row, column),
        }
    }

    pub(crate) fn opt_bool(&self, column: &str) -> SqlResult<Option<bool>> {
        match self {
            SqlRow::Sqlite(row) => opt_bool_from_sqlite_row(row, column),
            SqlRow::Postgres(row) => opt_bool_from_pg_row(row, column),
        }
    }

    pub(crate) fn timestamp(&self, column: &str) -> SqlResult<DateTime<Utc>> {
        match self {
            SqlRow::Sqlite(row) => {
                let raw: String = row.try_get(column).map_err(db_err)?;
                parse_utc_datetime(&raw)
            }
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn opt_timestamp(&self, column: &str) -> SqlResult<Option<DateTime<Utc>>> {
        match self {
            SqlRow::Sqlite(row) => {
                let raw: Option<String> = row.try_get(column).map_err(db_err)?;
                match raw {
                    Some(raw) if !raw.trim().is_empty() => parse_utc_datetime(&raw).map(Some),
                    Some(_) | None => Ok(None),
                }
            }
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn opt_json(&self, column: &str) -> SqlResult<Option<JsonValue>> {
        match self {
            SqlRow::Sqlite(row) => {
                let raw: Option<String> = row.try_get(column).map_err(db_err)?;
                match raw {
                    Some(raw) if !raw.trim().is_empty() => {
                        serde_json::from_str(&raw).map(Some).map_err(db_err)
                    }
                    Some(_) | None => Ok(None),
                }
            }
            SqlRow::Postgres(row) => {
                if let Ok(raw) = row.try_get::<Option<Json<JsonValue>>, _>(column) {
                    return Ok(raw.map(|value| value.0));
                }
                let raw: Option<String> = row.try_get(column).map_err(db_err)?;
                raw.filter(|value| !value.trim().is_empty())
                    .map(|value| serde_json::from_str(&value).map_err(db_err))
                    .transpose()
            }
        }
    }

    pub(crate) fn opt_bytes(&self, column: &str) -> SqlResult<Option<Vec<u8>>> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }

    pub(crate) fn bytes(&self, column: &str) -> SqlResult<Vec<u8>> {
        match self {
            SqlRow::Sqlite(row) => row.try_get(column).map_err(db_err),
            SqlRow::Postgres(row) => row.try_get(column).map_err(db_err),
        }
    }
}

type SqliteQuery<'q> = Query<'q, Sqlite, SqliteArguments>;
type PostgresQuery<'q> = Query<'q, Postgres, PgArguments>;

fn bind_sqlite<'q>(mut query: SqliteQuery<'q>, values: &'q [SqlArg]) -> SqliteQuery<'q> {
    for value in values {
        query = match value {
            SqlArg::Text(value) => query.bind(value),
            SqlArg::OptText(value) => query.bind(value),
            SqlArg::I32(value) => query.bind(i64::from(*value)),
            SqlArg::I64(value) => query.bind(*value),
            SqlArg::F64(value) => query.bind(*value),
            SqlArg::OptI32(value) => query.bind(value.map(i64::from)),
            SqlArg::OptI64(value) => query.bind(*value),
            SqlArg::OptF64(value) => query.bind(*value),
            SqlArg::Bool(value) => query.bind(if *value { 1_i64 } else { 0_i64 }),
            SqlArg::OptBool(value) => {
                query.bind(value.map(|value| if value { 1_i64 } else { 0_i64 }))
            }
            SqlArg::Timestamp(value) => query.bind(value.to_rfc3339()),
            SqlArg::OptTimestamp(value) => query.bind(value.map(|value| value.to_rfc3339())),
            SqlArg::Json(value) => query.bind(value.to_string()),
            SqlArg::OptJson(value) => query.bind(value.as_ref().map(JsonValue::to_string)),
            SqlArg::Bytes(value) => query.bind(value),
            SqlArg::OptBytes(value) => query.bind(value),
        };
    }
    query
}

fn bind_postgres<'q>(mut query: PostgresQuery<'q>, values: &'q [SqlArg]) -> PostgresQuery<'q> {
    for value in values {
        query = match value {
            SqlArg::Text(value) => query.bind(value),
            SqlArg::OptText(value) => query.bind(value),
            SqlArg::I32(value) => query.bind(*value),
            SqlArg::I64(value) => query.bind(*value),
            SqlArg::F64(value) => query.bind(*value),
            SqlArg::OptI32(value) => query.bind(*value),
            SqlArg::OptI64(value) => query.bind(*value),
            SqlArg::OptF64(value) => query.bind(*value),
            SqlArg::Bool(value) => query.bind(*value),
            SqlArg::OptBool(value) => query.bind(*value),
            SqlArg::Timestamp(value) => query.bind(*value),
            SqlArg::OptTimestamp(value) => query.bind(*value),
            SqlArg::Json(value) => query.bind(value.to_string()),
            SqlArg::OptJson(value) => query.bind(value.as_ref().map(JsonValue::to_string)),
            SqlArg::Bytes(value) => query.bind(value),
            SqlArg::OptBytes(value) => query.bind(value),
        };
    }
    query
}

fn render_sql(template: &str, dialect: PlaceholderDialect, bind_count: usize) -> SqlResult<String> {
    let placeholder_count = template.matches("{}").count();
    if placeholder_count != bind_count {
        return Err(StateError::Database(format!(
            "sql placeholder mismatch: expected {placeholder_count} bind(s), received {bind_count}"
        )));
    }

    let mut next_index = 1usize;
    let mut rendered = String::with_capacity(template.len() + bind_count * 2);
    let mut parts = template.split("{}").peekable();
    while let Some(part) = parts.next() {
        rendered.push_str(part);
        if parts.peek().is_some() {
            match dialect {
                PlaceholderDialect::Sqlite => rendered.push('?'),
                PlaceholderDialect::Postgres => {
                    rendered.push('$');
                    rendered.push_str(&next_index.to_string());
                    next_index += 1;
                }
            }
        }
    }
    Ok(rendered)
}

fn opt_text_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<Option<String>> {
    match row.try_get::<Option<String>, _>(column) {
        Ok(value) => Ok(value),
        Err(string_error) => match row.try_get::<Option<i64>, _>(column) {
            Ok(value) => Ok(value.map(|value| value.to_string())),
            Err(integer_error) => Err(StateError::Database(format!(
                "failed decode {column} as optional text: {string_error}; {integer_error}"
            ))),
        },
    }
}

fn opt_text_from_pg_row(row: &PgRow, column: &str) -> SqlResult<Option<String>> {
    match row.try_get::<Option<String>, _>(column) {
        Ok(value) => Ok(value),
        Err(string_error) => match row.try_get::<Option<i64>, _>(column) {
            Ok(value) => Ok(value.map(|value| value.to_string())),
            Err(integer_error) => Err(StateError::Database(format!(
                "failed decode {column} as optional text: {string_error}; {integer_error}"
            ))),
        },
    }
}

fn opt_i64_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<Option<i64>> {
    row.try_get(column).map_err(db_err)
}

fn opt_i64_from_pg_row(row: &PgRow, column: &str) -> SqlResult<Option<i64>> {
    row.try_get::<Option<i64>, _>(column).or_else(|_| {
        row.try_get::<Option<i32>, _>(column)
            .map(|value| value.map(i64::from))
            .or_else(|_| {
                row.try_get::<Option<i16>, _>(column)
                    .map(|value| value.map(i64::from))
            })
            .map_err(db_err)
    })
}

fn i64_from_pg_row(row: &PgRow, column: &str) -> SqlResult<i64> {
    row.try_get::<i64, _>(column).or_else(|_| {
        row.try_get::<i32, _>(column)
            .map(i64::from)
            .or_else(|_| row.try_get::<i16, _>(column).map(i64::from))
            .map_err(db_err)
    })
}

fn i32_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<i32> {
    let value: i64 = row.try_get(column).map_err(db_err)?;
    i32_from_i64(column, value)
}

fn i32_from_pg_row(row: &PgRow, column: &str) -> SqlResult<i32> {
    row.try_get::<i32, _>(column).or_else(|_| {
        row.try_get::<i64, _>(column)
            .map_err(db_err)
            .and_then(|value| i32_from_i64(column, value))
    })
}

fn opt_i32_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<Option<i32>> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(db_err)?
        .map(|value| i32_from_i64(column, value))
        .transpose()
}

fn opt_i32_from_pg_row(row: &PgRow, column: &str) -> SqlResult<Option<i32>> {
    row.try_get::<Option<i32>, _>(column).or_else(|_| {
        row.try_get::<Option<i64>, _>(column)
            .map_err(db_err)
            .and_then(|value| value.map(|value| i32_from_i64(column, value)).transpose())
    })
}

fn i32_from_i64(column: &str, value: i64) -> SqlResult<i32> {
    i32::try_from(value).map_err(|_| {
        StateError::Database(format!(
            "value out of range for i32 column {column}: {value}"
        ))
    })
}

fn bool_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<bool> {
    let value: i64 = row.try_get(column).map_err(db_err)?;
    Ok(value != 0)
}

fn bool_from_pg_row(row: &PgRow, column: &str) -> SqlResult<bool> {
    row.try_get(column).map_err(db_err)
}

fn opt_bool_from_sqlite_row(row: &SqliteRow, column: &str) -> SqlResult<Option<bool>> {
    let value: Option<i64> = row.try_get(column).map_err(db_err)?;
    Ok(value.map(|value| value != 0))
}

fn opt_bool_from_pg_row(row: &PgRow, column: &str) -> SqlResult<Option<bool>> {
    row.try_get(column).map_err(db_err)
}

/// SQLite extended result codes that mean "retry later, the DB is momentarily
/// busy/locked": SQLITE_BUSY (5) and its BUSY_RECOVERY/BUSY_SNAPSHOT/BUSY_TIMEOUT
/// variants (261/517/773), plus SQLITE_LOCKED (6) and LOCKED_SHAREDCACHE (262).
/// These are the ONLY codes safe to spin on — a disk I/O error (e.g. code 522,
/// SQLITE_IOERR_SHORT_READ) must surface immediately, not retry for 120s.
const SQLITE_BUSY_TRANSIENT_CODES: [u32; 6] = [5, 261, 517, 773, 6, 262];

pub(crate) fn is_transient_sqlite_busy(error: &StateError) -> bool {
    let StateError::Database(message) = error else {
        return false;
    };

    let normalized = message.to_ascii_lowercase();
    normalized.contains("database is locked")
        || normalized.contains("database table is locked")
        || normalized.contains("database schema is locked")
        || normalized.contains("sqlite_busy")
        || normalized.contains("busy_snapshot")
        // Boundary-aware numeric match on both marker shapes. A plain
        // `contains("code: 5")` would also match "code: 522" (a disk I/O error)
        // and spin futilely, so parse the digit run and compare exact codes.
        || contains_sqlite_code(&normalized, "sqlite_code=", SQLITE_BUSY_TRANSIENT_CODES.as_slice())
        || contains_sqlite_code(&normalized, "code: ", SQLITE_BUSY_TRANSIENT_CODES.as_slice())
}

/// Scan `haystack` for every occurrence of `marker`, parse the contiguous digit
/// run that immediately follows it as a `u32`, and return true if any parsed
/// value is in `codes`. The match is boundary-aware: the digit run ends at the
/// first non-digit, so `"code: 5)"` yields 5 while `"code: 522"` yields 522.
fn contains_sqlite_code(haystack: &str, marker: &str, codes: &[u32]) -> bool {
    let mut rest = haystack;
    while let Some(pos) = rest.find(marker) {
        let after = &rest[pos + marker.len()..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(code) = digits.parse::<u32>()
            && codes.contains(&code)
        {
            return true;
        }
        // Advance past this marker to find any later occurrences.
        rest = &after[digits.len()..];
    }
    false
}

/// Postgres errors that are always safe to retry because the failed transaction
/// is guaranteed to have rolled back: serialization failures (SQLSTATE 40001)
/// and deadlocks (40P01). The code is preserved by [`pg_db_err`].
pub(crate) fn is_postgres_rolled_back_transient(error: &StateError) -> bool {
    let StateError::Database(message) = error else {
        return false;
    };
    let normalized = message.to_ascii_lowercase();
    normalized.contains("sqlstate=40001")
        || normalized.contains("sqlstate=40p01")
        || normalized.contains("deadlock detected")
        || normalized.contains("could not serialize access")
}

/// Postgres connection-level failures (SQLSTATE class 08 or a dropped socket).
/// Retrying is safe for reads and for whole transactions (a lost connection
/// aborts an open transaction) but NOT for a bare autocommit write, whose
/// commit outcome is unknown.
pub(crate) fn is_postgres_connection_error(error: &StateError) -> bool {
    let StateError::Database(message) = error else {
        return false;
    };
    let normalized = message.to_ascii_lowercase();
    // `connection_error` is the marker `pg_db_err` attaches to transport-level
    // sqlx errors (Io/Tls/Protocol) that have no SQLSTATE — e.g. an abrupt EOF
    // from a dropped backend or a stale pooled connection.
    normalized.contains("connection_error")
        || normalized.contains("sqlstate=08")
        || normalized.contains("connection reset")
        || normalized.contains("connection closed")
        || normalized.contains("broken pipe")
        || normalized.contains("terminating connection")
        || normalized.contains("server closed the connection")
        || normalized.contains("no connection to the server")
}

async fn run_with_postgres_retries<T, Op, Fut>(
    op_name: &str,
    retry_connection_errors: bool,
    mut op: Op,
) -> SqlResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = SqlResult<T>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retryable = is_postgres_rolled_back_transient(&error)
                    || (retry_connection_errors && is_postgres_connection_error(&error));
                if retryable && attempt < POSTGRES_TRANSIENT_RETRY_DELAYS.len() {
                    let delay = POSTGRES_TRANSIENT_RETRY_DELAYS[attempt];
                    tracing::debug!(
                        operation = op_name,
                        attempt = attempt + 1,
                        retry_after_ms = delay.as_millis() as u64,
                        error = %error,
                        "retrying transient postgres error"
                    );
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                } else {
                    return Err(error);
                }
            }
        }
    }
}

/// Which phase of a Postgres transaction produced an error. The retry decision
/// keys on this flag *structurally* — never on the error message — because a
/// connection-class failure at COMMIT stringifies with the same `connection_error`
/// marker as one before COMMIT, yet only the pre-commit case is safe to retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxPhase {
    /// `pool.begin()` or a statement inside `op(&mut tx)` failed. Nothing was
    /// committed, so the whole transaction can be re-`begin`'d and re-applied.
    BeforeCommit,
    /// `tx.commit()` failed. If the failure is not guaranteed-rolled-back, the
    /// commit outcome is UNKNOWN and the transaction must NOT be retried.
    Commit,
}

/// A transaction error tagged with the phase it originated in.
struct PostgresTxError {
    phase: TxPhase,
    error: StateError,
}

impl PostgresTxError {
    fn new(phase: TxPhase, error: StateError) -> Self {
        Self { phase, error }
    }
}

/// Pure retry-eligibility decision for a phase-tagged Postgres transaction error.
///
/// * `BeforeCommit` — nothing durable was applied, so retry on either a
///   guaranteed-rolled-back class-40 abort or any connection-level failure.
/// * `Commit` — retry ONLY when the failure is guaranteed rolled back
///   (serialization/deadlock raised at COMMIT). A connection-class error here
///   means the commit's outcome is unknown, so it is not retried.
fn postgres_tx_retry_eligible(phase: TxPhase, error: &StateError) -> bool {
    match phase {
        TxPhase::BeforeCommit => {
            is_postgres_rolled_back_transient(error) || is_postgres_connection_error(error)
        }
        TxPhase::Commit => is_postgres_rolled_back_transient(error),
    }
}

/// Run a Postgres transaction (`begin` + `op` + `commit`) with phase-aware
/// transient retries bounded by [`POSTGRES_TRANSIENT_RETRY_DELAYS`]. The closure
/// tags each failure with the phase it came from via [`PostgresTxError`]; the
/// retry decision consults that flag through [`postgres_tx_retry_eligible`], so a
/// connection error at COMMIT (outcome unknown) is surfaced rather than retried.
async fn run_postgres_transaction_with_retries<T, Op, Fut>(
    op_name: &str,
    mut op: Op,
) -> SqlResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PostgresTxError>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(PostgresTxError { phase, error }) => {
                let eligible = postgres_tx_retry_eligible(phase, &error);
                if eligible && attempt < POSTGRES_TRANSIENT_RETRY_DELAYS.len() {
                    let delay = POSTGRES_TRANSIENT_RETRY_DELAYS[attempt];
                    tracing::debug!(
                        operation = op_name,
                        attempt = attempt + 1,
                        phase = ?phase,
                        retry_after_ms = delay.as_millis() as u64,
                        error = %error,
                        "retrying transient postgres transaction error"
                    );
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                } else if phase == TxPhase::Commit && is_postgres_connection_error(&error) {
                    // The connection dropped after COMMIT was sent but before the
                    // ack: the server may already have committed. Do not retry —
                    // re-running would duplicate the writes. Make the ambiguity
                    // explicit so callers/logs can tell this apart from a clean
                    // failure that applied nothing.
                    return Err(commit_outcome_unknown(op_name, error));
                } else {
                    return Err(error);
                }
            }
        }
    }
}

/// Rewrite a commit-phase connection error so its message clearly conveys that
/// the commit's outcome is unknown (the write may or may not have landed).
fn commit_outcome_unknown(op_name: &str, error: StateError) -> StateError {
    StateError::Database(format!(
        "commit_outcome_unknown; transaction `{op_name}` lost its connection at COMMIT, so it may or may not have committed: {error}"
    ))
}

pub(crate) async fn run_with_sqlite_busy_retries<T, Op, Fut>(
    operation_name: &str,
    mut operation: Op,
) -> SqlResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = SqlResult<T>>,
{
    run_with_sqlite_busy_retries_with_deadline(
        operation_name,
        SQLITE_BUSY_RETRY_HARD_CAP,
        &mut operation,
    )
    .await
}

pub(crate) async fn run_with_sqlite_busy_retries_with_deadline<T, Op, Fut>(
    operation_name: &str,
    hard_cap: Duration,
    operation: &mut Op,
) -> SqlResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = SqlResult<T>>,
{
    let started_at = tokio::time::Instant::now();
    let mut attempt = 0usize;

    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if is_transient_sqlite_busy(&error) => {
                let elapsed = started_at.elapsed();
                if elapsed >= hard_cap {
                    tracing::warn!(
                        attempts = attempt,
                        elapsed_ms = elapsed.as_millis(),
                        error = %error,
                        operation = operation_name,
                        "serialized sqlite writer: retry deadline exhausted"
                    );
                    return Err(StateError::Database(format!(
                        "serialized sqlite writer: retry deadline exceeded for operation `{operation_name}` after {attempt} attempts over {}ms: {error}",
                        elapsed.as_millis()
                    )));
                }

                let scheduled_delay = SQLITE_BUSY_RETRY_DELAYS
                    [attempt.min(SQLITE_BUSY_RETRY_DELAYS.len().saturating_sub(1))];
                let remaining = hard_cap.saturating_sub(elapsed);
                let delay = scheduled_delay.min(remaining);
                tracing::debug!(
                    attempt = attempt + 1,
                    retry_after_ms = delay.as_millis(),
                    elapsed_ms = elapsed.as_millis(),
                    error = %error,
                    operation = operation_name,
                    "serialized sqlite writer: retrying transient sqlite busy"
                );
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn parse_utc_datetime(raw: &str) -> SqlResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(db_err)
}

pub(crate) fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

/// Map a Postgres `sqlx::Error` to `StateError`, prefixing the SQLSTATE code as
/// `sqlstate=<code>;` so the transient-retry classifiers can recognize
/// serialization/deadlock/connection failures after the error is stringified.
pub(crate) fn pg_db_err(error: sqlx::Error) -> StateError {
    if let Some(code) = error
        .as_database_error()
        .and_then(|db| db.code())
        .map(|code| code.into_owned())
    {
        return StateError::Database(format!("sqlstate={code}; {error}"));
    }
    // Transport/pool failures carry no SQLSTATE. Crucially, an abruptly dropped
    // backend (crash, kill, proxy/idle-timeout FIN, container restart) surfaces
    // as `Error::Io(UnexpectedEof)` — NOT a `DatabaseError` — so it must be
    // classified by the error variant, not by matching locale-dependent io
    // message text. Tag it with a stable marker the retry classifier keys on.
    if matches!(
        &error,
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_)
    ) {
        return StateError::Database(format!("connection_error; {error}"));
    }
    // A pool acquire timeout never sent a statement, so it is safe to retry for
    // reads and whole transactions (both re-acquire on the next attempt). It is
    // NEVER retried for a bare autocommit write because those run with retry
    // mode `false`, which ignores connection-class errors.
    if matches!(&error, sqlx::Error::PoolTimedOut) {
        return StateError::Database(format!("connection_error; {error}"));
    }
    StateError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_sqlite_placeholders() {
        let sql = render_sql(
            "SELECT * FROM jobs WHERE id = {} AND status = {}",
            PlaceholderDialect::Sqlite,
            2,
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM jobs WHERE id = ? AND status = ?");
    }

    #[test]
    fn render_postgres_placeholders() {
        let sql = render_sql(
            "SELECT * FROM jobs WHERE id = {} AND status = {}",
            PlaceholderDialect::Postgres,
            2,
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM jobs WHERE id = $1 AND status = $2");
    }

    #[test]
    fn render_sql_rejects_placeholder_mismatch() {
        let error = render_sql(
            "SELECT * FROM jobs WHERE id = {}",
            PlaceholderDialect::Sqlite,
            0,
        )
        .unwrap_err();
        assert!(error.to_string().contains("sql placeholder mismatch"));
    }

    #[test]
    fn detects_transient_sqlite_busy_errors() {
        assert!(is_transient_sqlite_busy(&StateError::Database(
            "database is locked".to_string()
        )));
        assert!(!is_transient_sqlite_busy(&StateError::Database(
            "syntax error".to_string()
        )));
    }

    #[test]
    fn detects_rolled_back_postgres_transients() {
        assert!(is_postgres_rolled_back_transient(&StateError::Database(
            "sqlstate=40P01; deadlock detected".to_string()
        )));
        assert!(is_postgres_rolled_back_transient(&StateError::Database(
            "sqlstate=40001; could not serialize access".to_string()
        )));
        // A connection drop is not "guaranteed rolled back" for a bare write.
        assert!(!is_postgres_rolled_back_transient(&StateError::Database(
            "sqlstate=08006; connection reset".to_string()
        )));
        assert!(!is_postgres_rolled_back_transient(&StateError::Database(
            "syntax error".to_string()
        )));
    }

    #[test]
    fn detects_postgres_connection_errors() {
        assert!(is_postgres_connection_error(&StateError::Database(
            "sqlstate=08006; server closed the connection unexpectedly".to_string()
        )));
        assert!(is_postgres_connection_error(&StateError::Database(
            "connection reset by peer".to_string()
        )));
        assert!(!is_postgres_connection_error(&StateError::Database(
            "sqlstate=40001; could not serialize access".to_string()
        )));
    }

    #[test]
    fn classifies_abrupt_eof_as_connection_error() {
        // An abruptly dropped Postgres backend (crash/kill/proxy FIN) surfaces
        // as Error::Io(UnexpectedEof) with no SQLSTATE; it must still be
        // classified as a connection error so reads/transactions retry.
        let mapped = pg_db_err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "expected to read 5 bytes, got 0 bytes at EOF",
        )));
        assert!(is_postgres_connection_error(&mapped));
        assert!(!is_postgres_rolled_back_transient(&mapped));
    }

    #[test]
    fn classifies_pool_timeout_as_connection_error() {
        // A pool acquire timeout sent no statement, so it is retryable for reads
        // and whole transactions (both re-acquire); it must be tagged like other
        // connection errors so those paths retry.
        let mapped = pg_db_err(sqlx::Error::PoolTimedOut);
        assert!(is_postgres_connection_error(&mapped));
        assert!(!is_postgres_rolled_back_transient(&mapped));
    }

    #[test]
    fn phase_aware_retry_op_phase_connection_error_is_retried() {
        // A connection error before COMMIT applied nothing durable → retry.
        let error = StateError::Database("connection_error; unexpected end of file".to_string());
        assert!(postgres_tx_retry_eligible(TxPhase::BeforeCommit, &error));
    }

    #[test]
    fn phase_aware_retry_commit_phase_connection_error_is_not_retried() {
        // A connection error AT COMMIT leaves the outcome unknown → do NOT retry,
        // even though the message carries the `connection_error` marker.
        let error = StateError::Database("connection_error; broken pipe".to_string());
        assert!(!postgres_tx_retry_eligible(TxPhase::Commit, &error));
    }

    #[test]
    fn phase_aware_retry_commit_phase_serialization_is_retried() {
        // A 40001 raised at COMMIT is guaranteed rolled back → retry.
        let error = StateError::Database("sqlstate=40001; could not serialize access".to_string());
        assert!(postgres_tx_retry_eligible(TxPhase::Commit, &error));
        // ...and it is retried before COMMIT too.
        assert!(postgres_tx_retry_eligible(TxPhase::BeforeCommit, &error));
    }

    #[test]
    fn phase_aware_retry_op_phase_deadlock_is_retried() {
        let error = StateError::Database("sqlstate=40P01; deadlock detected".to_string());
        assert!(postgres_tx_retry_eligible(TxPhase::BeforeCommit, &error));
        assert!(postgres_tx_retry_eligible(TxPhase::Commit, &error));
    }

    #[tokio::test]
    async fn phase_aware_retry_bounds_op_phase_attempts() {
        // An always-failing op-phase connection error should be attempted exactly
        // 1 + POSTGRES_TRANSIENT_RETRY_DELAYS.len() times, then surface.
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result: SqlResult<()> = run_postgres_transaction_with_retries("test_op", || {
            let attempts = &attempts;
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(PostgresTxError::new(
                    TxPhase::BeforeCommit,
                    StateError::Database("connection_error; broken pipe".to_string()),
                ))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1 + POSTGRES_TRANSIENT_RETRY_DELAYS.len()
        );
    }

    #[tokio::test]
    async fn phase_aware_retry_commit_connection_error_surfaces_immediately() {
        // A commit-phase connection error must NOT be retried and must surface a
        // message that conveys the commit outcome is unknown.
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result: SqlResult<()> = run_postgres_transaction_with_retries("test_commit", || {
            let attempts = &attempts;
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(PostgresTxError::new(
                    TxPhase::Commit,
                    StateError::Database(
                        "connection_error; server closed the connection".to_string(),
                    ),
                ))
            }
        })
        .await;
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let StateError::Database(message) = result.unwrap_err() else {
            panic!("expected a Database error");
        };
        assert!(
            message.contains("commit_outcome_unknown"),
            "message should flag commit-outcome-unknown, got: {message}"
        );
    }

    #[test]
    fn sqlite_busy_classifier_is_boundary_aware() {
        // BUSY (5) and LOCKED-family codes match on both marker shapes...
        assert!(is_transient_sqlite_busy(&StateError::Database(
            "error returned from database: (code: 5) database is locked".to_string()
        )));
        assert!(is_transient_sqlite_busy(&StateError::Database(
            "(code: 517) busy_snapshot".to_string()
        )));
        assert!(is_transient_sqlite_busy(&StateError::Database(
            "sqlite_code=261 busy_recovery".to_string()
        )));
        assert!(is_transient_sqlite_busy(&StateError::Database(
            "sqlite_code=6 locked".to_string()
        )));
        // ...but a disk I/O error (522, SQLITE_IOERR_SHORT_READ) must NOT — it
        // would otherwise spin for the whole 120s hard cap before surfacing.
        assert!(!is_transient_sqlite_busy(&StateError::Database(
            "error returned from database: (code: 522) disk I/O error".to_string()
        )));
        // A truncated/partial numeric prefix must not match by accident.
        assert!(!is_transient_sqlite_busy(&StateError::Database(
            "(code: 50) something".to_string()
        )));
        assert!(!is_transient_sqlite_busy(&StateError::Database(
            "sqlite_code=522 disk".to_string()
        )));
    }

    #[test]
    fn contains_sqlite_code_parses_digit_boundary() {
        assert!(contains_sqlite_code("(code: 5)", "code: ", &[5]));
        assert!(contains_sqlite_code("code: 517 foo", "code: ", &[517]));
        assert!(!contains_sqlite_code("code: 522", "code: ", &[5, 517]));
        assert!(!contains_sqlite_code("code: 50", "code: ", &[5]));
        // A later valid marker still matches after an earlier non-matching one.
        assert!(contains_sqlite_code(
            "code: 522 then code: 5)",
            "code: ",
            &[5]
        ));
    }
}
