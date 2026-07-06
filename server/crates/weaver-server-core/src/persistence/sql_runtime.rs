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
                // Retry the whole transaction: a serialization/deadlock abort or
                // a dropped connection invalidates the open transaction, so it
                // must be re-`begin`'d. `op` is `Fn`, so re-invocation is sound.
                run_with_postgres_retries(op_name, true, || async {
                    let started = Instant::now();
                    let mut tx = SqlTx::Postgres(pool.begin().await.map_err(pg_db_err)?);
                    let result = {
                        let future = op(&mut tx);
                        future.await?
                    };
                    tx.commit().await?;
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

pub(crate) fn is_transient_sqlite_busy(error: &StateError) -> bool {
    let StateError::Database(message) = error else {
        return false;
    };

    let normalized = message.to_ascii_lowercase();
    normalized.contains("sqlite_code=5")
        || normalized.contains("sqlite_code=517")
        || normalized.contains("database is locked")
        || normalized.contains("database table is locked")
        || normalized.contains("database schema is locked")
        || normalized.contains("sqlite_busy")
        || normalized.contains("busy_snapshot")
        || normalized.contains("code: 5")
        || normalized.contains("code: 517")
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
}
