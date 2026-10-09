use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqliteRow};
use sqlx::{Acquire, Column, Connection, Postgres, Row, Sqlite, TypeInfo, ValueRef};

use super::catalog::{
    BACKUP_TABLE_CATALOG, BackupTableClassification, catalog_tables, export_query,
    is_engine_internal_table, is_optional_catalog_table, quote_identifier,
};
use crate::persistence::sql_runtime::{SqlConn, StoreDatastore};
use crate::schema_migrations::{egress_quotas_v53, script_instances_v55};
use crate::security::RuntimeSecurityConfig;
use crate::{Database, StateError};

pub(crate) const EXPORT_BATCH_SIZE: i64 = 1_000;
const BLOB_MARKER_TYPE: &str = "__weaver_type";
const BLOB_MARKER_BASE64: &str = "base64";
type NdjsonRow = Result<(usize, JsonMap<String, JsonValue>), StateError>;

struct NdjsonRows {
    reader: BufReader<File>,
    display: String,
    line_number: usize,
    line: Vec<u8>,
    line_limit: usize,
}

impl Iterator for NdjsonRows {
    type Item = NdjsonRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line.clear();
            let read = match read_bounded_line(&mut self.reader, &mut self.line, self.line_limit) {
                Ok(read) => read,
                Err(error) => return Some(Err(error)),
            };
            if read == 0 {
                return None;
            }
            let line_number = self.line_number;
            self.line_number += 1;
            if self.line == b"\n" {
                continue;
            }
            let value: JsonValue = match serde_json::from_slice(&self.line) {
                Ok(value) => value,
                Err(error) => return Some(Err(db_err(error))),
            };
            let Some(object) = value.as_object().cloned() else {
                return Some(Err(StateError::Database(format!(
                    "backup row {}:{} is not an object",
                    self.display,
                    line_number + 1
                ))));
            };
            return Some(Ok((line_number, object)));
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TablePartMetadata {
    pub rows: u64,
    pub columns: Vec<String>,
    pub checksum: String,
}

pub(crate) struct LogicalBackupExport {
    pub staging: tempfile::TempDir,
    pub source_engine: String,
    pub schema_version: i64,
    pub tables: BTreeMap<String, TablePartMetadata>,
}

impl Database {
    #[cfg(test)]
    pub(crate) fn export_logical_backup(&self) -> Result<LogicalBackupExport, StateError> {
        self.export_logical_backup_cancellable(super::archive::BackupCancellation::new())
    }

    pub(super) fn export_logical_backup_cancellable(
        &self,
        cancellation: super::archive::BackupCancellation,
    ) -> Result<LogicalBackupExport, StateError> {
        let staging = super::create_backup_temp_dir().map_err(db_err)?;
        let tables_dir = staging.path().join("tables");
        std::fs::create_dir_all(&tables_dir).map_err(db_err)?;
        let schema_version = self.schema_version()?;
        let datastore = self.datastore();
        let tables = match datastore {
            StoreDatastore::Sqlite { .. } => {
                let tables_dir = tables_dir.clone();
                let path = self
                    .database_target()
                    .sqlite_path()?
                    .ok_or_else(|| StateError::Database("SQLite backup has no path".into()))?;
                self.run_sql_blocking_local(move || async move {
                    export_sqlite(&path, &tables_dir, false, &cancellation).await
                })?
            }
            StoreDatastore::Postgres { pool } => {
                let tables_dir = tables_dir.clone();
                self.run_sql_blocking_read(async move {
                    export_postgres(pool, &tables_dir, false, &cancellation).await
                })?
            }
        };
        Ok(LogicalBackupExport {
            staging,
            source_engine: self.datastore().engine().as_str().to_string(),
            schema_version,
            tables,
        })
    }

    pub(crate) fn import_logical_backup(
        &self,
        tables_dir: &Path,
        expected: &BTreeMap<String, TablePartMetadata>,
        source_schema_version: i64,
    ) -> Result<(), StateError> {
        let allow_older_catalog = source_schema_version < self.schema_version()?;
        let datastore = self.datastore();
        match datastore {
            StoreDatastore::Sqlite { pool, .. } => {
                let tables_dir = tables_dir.to_path_buf();
                let expected = expected.clone();
                self.run_sql_blocking_local(move || async move {
                    import_sqlite(
                        pool,
                        &tables_dir,
                        &expected,
                        allow_older_catalog,
                        source_schema_version,
                    )
                    .await
                })
            }
            StoreDatastore::Postgres { pool } => {
                let tables_dir = tables_dir.to_path_buf();
                let expected = expected.clone();
                self.run_sql_blocking_read(async move {
                    import_postgres(
                        pool,
                        &tables_dir,
                        &expected,
                        allow_older_catalog,
                        source_schema_version,
                    )
                    .await
                })
            }
        }
    }

    pub(crate) fn validate_backup_catalog(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        match datastore {
            StoreDatastore::Sqlite { pool, .. } => {
                self.run_sql_blocking_local(move || async move {
                    let mut conn = pool.acquire().await.map_err(db_err)?;
                    validate_sqlite_catalog(&mut conn).await.map(|_| ())
                })
            }
            StoreDatastore::Postgres { pool } => self.run_sql_blocking_read(async move {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                validate_postgres_catalog(&mut conn).await.map(|_| ())
            }),
        }
    }

    pub(crate) fn validate_logical_backup_import(
        &self,
        expected: &BTreeMap<String, TablePartMetadata>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        match datastore {
            StoreDatastore::Sqlite { pool, .. } => {
                let expected = expected.clone();
                self.run_sql_blocking_local(move || async move {
                    let mut conn = pool.acquire().await.map_err(db_err)?;
                    validate_sqlite_counts(&mut conn, &expected).await?;
                    let violations: i64 =
                        sqlx::query_scalar("SELECT COUNT(*) FROM pragma_foreign_key_check")
                            .fetch_one(&mut *conn)
                            .await
                            .map_err(db_err)?;
                    if violations == 0 {
                        Ok(())
                    } else {
                        Err(StateError::Database(format!(
                            "restored database has {violations} foreign-key violations"
                        )))
                    }
                })
            }
            StoreDatastore::Postgres { pool } => {
                let expected = expected.clone();
                self.run_sql_blocking_read(async move {
                    let mut conn = pool.acquire().await.map_err(db_err)?;
                    let mut tx = conn.begin().await.map_err(db_err)?;
                    validate_postgres_counts(&mut tx, &expected).await?;
                    tx.rollback().await.map_err(db_err)
                })
            }
        }
    }
}

async fn export_sqlite(
    database_path: &Path,
    tables_dir: &Path,
    allow_older_catalog: bool,
    cancellation: &super::archive::BackupCancellation,
) -> Result<BTreeMap<String, TablePartMetadata>, StateError> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .read_only(true)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(30));
    let mut conn = sqlx::SqliteConnection::connect_with(&options)
        .await
        .map_err(db_err)?;
    let actual = validate_sqlite_catalog_mode(&mut conn, allow_older_catalog).await?;
    sqlx::query("BEGIN")
        .execute(&mut conn)
        .await
        .map_err(db_err)?;
    let result = async {
        let mut tables =
            ordered_sqlite_tables(&mut conn, &actual, &[BackupTableClassification::Export]).await?;
        if allow_older_catalog {
            append_unclassified_tables(&mut tables, &actual)?;
        }
        let mut parts = BTreeMap::new();
        for table in tables {
            let part = export_sqlite_table(&mut conn, &table, tables_dir, cancellation).await?;
            parts.insert(table, part);
        }
        Ok::<_, StateError>(parts)
    }
    .await;
    let rollback = sqlx::query("ROLLBACK").execute(&mut conn).await;
    match (result, rollback) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(db_err(error)),
        (Ok(parts), Ok(_)) => Ok(parts),
    }
}

async fn export_postgres(
    pool: sqlx::PgPool,
    tables_dir: &Path,
    allow_older_catalog: bool,
    cancellation: &super::archive::BackupCancellation,
) -> Result<BTreeMap<String, TablePartMetadata>, StateError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    let actual = validate_postgres_catalog_mode(&mut conn, allow_older_catalog).await?;
    let mut tx = conn.begin().await.map_err(db_err)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    // A table held exclusively elsewhere fails the export instead of stalling it.
    sqlx::query("SET LOCAL lock_timeout = '60s'")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    let mut tables =
        ordered_postgres_tables(&mut tx, &actual, &[BackupTableClassification::Export]).await?;
    if allow_older_catalog {
        append_unclassified_tables(&mut tables, &actual)?;
    }
    let mut parts = BTreeMap::new();
    for table in tables {
        let part = export_postgres_table(&mut tx, &table, tables_dir, cancellation).await?;
        parts.insert(table, part);
    }
    tx.rollback().await.map_err(db_err)?;
    Ok(parts)
}

async fn validate_sqlite_catalog(
    conn: &mut sqlx::SqliteConnection,
) -> Result<BTreeSet<String>, StateError> {
    validate_sqlite_catalog_mode(conn, false).await
}

async fn validate_sqlite_catalog_mode(
    conn: &mut sqlx::SqliteConnection,
    allow_older_catalog: bool,
) -> Result<BTreeSet<String>, StateError> {
    let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let actual = rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .filter(|table| !is_engine_internal_table(table))
        .collect::<BTreeSet<_>>();
    validate_actual_tables(&actual, allow_older_catalog)?;
    Ok(actual)
}

async fn validate_postgres_catalog(
    conn: &mut sqlx::PgConnection,
) -> Result<BTreeSet<String>, StateError> {
    validate_postgres_catalog_mode(conn, false).await
}

async fn validate_postgres_catalog_mode(
    conn: &mut sqlx::PgConnection,
    allow_older_catalog: bool,
) -> Result<BTreeSet<String>, StateError> {
    let rows = sqlx::query(
        "SELECT table_name
           FROM information_schema.tables
          WHERE table_schema = current_schema()
            AND table_type = 'BASE TABLE'
          ORDER BY table_name",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(db_err)?;
    let actual = rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("table_name").ok())
        .collect::<BTreeSet<_>>();
    validate_actual_tables(&actual, allow_older_catalog)?;
    Ok(actual)
}

fn validate_actual_tables(
    actual: &BTreeSet<String>,
    allow_older_catalog: bool,
) -> Result<(), StateError> {
    let classified = BACKUP_TABLE_CATALOG
        .iter()
        .map(|entry| entry.table.to_string())
        .collect::<BTreeSet<_>>();
    if classified.len() != BACKUP_TABLE_CATALOG.len() {
        return Err(StateError::Database(
            "backup catalog classifies a table more than once".into(),
        ));
    }
    let unclassified = actual.difference(&classified).cloned().collect::<Vec<_>>();
    let nonexistent = classified
        .difference(actual)
        .filter(|table| !is_optional_catalog_table(table))
        .cloned()
        .collect::<Vec<_>>();
    if allow_older_catalog || (unclassified.is_empty() && nonexistent.is_empty()) {
        Ok(())
    } else {
        Err(StateError::Database(format!(
            "backup catalog mismatch: unclassified [{}], nonexistent [{}]",
            unclassified.join(", "),
            nonexistent.join(", ")
        )))
    }
}

fn append_unclassified_tables(
    tables: &mut Vec<String>,
    actual: &BTreeSet<String>,
) -> Result<(), StateError> {
    let classified = BACKUP_TABLE_CATALOG
        .iter()
        .map(|entry| entry.table)
        .collect::<BTreeSet<_>>();
    for table in actual {
        if !classified.contains(table.as_str()) {
            if !table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(StateError::Database(
                    "legacy table name cannot be represented safely in a backup".into(),
                ));
            }
            tables.push(table.clone());
        }
    }
    Ok(())
}

fn check_export_cancellation(
    cancellation: &super::archive::BackupCancellation,
) -> Result<(), StateError> {
    if cancellation.is_cancelled() {
        Err(StateError::Database("backup export cancelled".into()))
    } else {
        Ok(())
    }
}

async fn export_sqlite_table(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
    tables_dir: &Path,
    cancellation: &super::archive::BackupCancellation,
) -> Result<TablePartMetadata, StateError> {
    check_export_cancellation(cancellation)?;
    let columns = sqlite_table_columns(conn, table).await?;
    let order = sqlite_row_order(conn, table).await?;
    let base = export_query(table, &columns);
    let query = format!("{base} ORDER BY {order} LIMIT ? OFFSET ?");
    let path = tables_dir.join(format!("{table}.ndjson"));
    let mut output = ExportTableWriter::new(&path)?;
    let mut offset = 0_i64;
    loop {
        check_export_cancellation(cancellation)?;
        let batch = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .bind(EXPORT_BATCH_SIZE)
            .bind(offset)
            .fetch_all(&mut *conn)
            .await
            .map_err(db_err)?;
        if batch.is_empty() {
            break;
        }
        offset += i64::try_from(batch.len()).map_err(db_err)?;
        for row in batch {
            output.write(encode_sqlite_row(&row)?)?;
        }
    }
    output.finish(columns)
}

async fn export_postgres_table(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    table: &str,
    tables_dir: &Path,
    cancellation: &super::archive::BackupCancellation,
) -> Result<TablePartMetadata, StateError> {
    check_export_cancellation(cancellation)?;
    let columns = postgres_table_columns(tx, table).await?;
    let order = postgres_row_order(tx, table).await?;
    let base = export_query(table, &columns);
    let query = format!("{base} ORDER BY {order} LIMIT $1 OFFSET $2");
    let path = tables_dir.join(format!("{table}.ndjson"));
    let mut output = ExportTableWriter::new(&path)?;
    let mut offset = 0_i64;
    loop {
        check_export_cancellation(cancellation)?;
        let batch = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .bind(EXPORT_BATCH_SIZE)
            .bind(offset)
            .fetch_all(&mut **tx)
            .await
            .map_err(db_err)?;
        if batch.is_empty() {
            break;
        }
        offset += i64::try_from(batch.len()).map_err(db_err)?;
        for row in batch {
            output.write(encode_postgres_row(&row)?)?;
        }
    }
    output.finish(columns)
}

struct ExportTableWriter {
    writer: BufWriter<File>,
    hasher: blake3::Hasher,
    line: Vec<u8>,
    rows: u64,
}

impl ExportTableWriter {
    fn new(path: &Path) -> Result<Self, StateError> {
        Ok(Self {
            writer: BufWriter::new(File::create(path).map_err(db_err)?),
            hasher: blake3::Hasher::new(),
            line: Vec::with_capacity(16 * 1024),
            rows: 0,
        })
    }

    fn write(&mut self, value: JsonValue) -> Result<(), StateError> {
        self.line.clear();
        serde_json::to_writer(&mut self.line, &value).map_err(db_err)?;
        self.line.push(b'\n');
        self.writer.write_all(&self.line).map_err(db_err)?;
        self.hasher.update(&self.line);
        self.rows += 1;
        Ok(())
    }

    fn finish(mut self, columns: Vec<String>) -> Result<TablePartMetadata, StateError> {
        self.writer.flush().map_err(db_err)?;
        Ok(TablePartMetadata {
            rows: self.rows,
            columns,
            checksum: self.hasher.finalize().to_hex().to_string(),
        })
    }
}

fn encode_sqlite_row(row: &SqliteRow) -> Result<JsonValue, StateError> {
    let mut object = JsonMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(index).map_err(db_err)?;
        let value = if raw.is_null() {
            JsonValue::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => JsonValue::from(row.try_get::<i64, _>(index).map_err(db_err)?),
                "REAL" => JsonValue::from(row.try_get::<f64, _>(index).map_err(db_err)?),
                "BLOB" => encode_blob(&row.try_get::<Vec<u8>, _>(index).map_err(db_err)?),
                _ => JsonValue::String(row.try_get::<String, _>(index).map_err(db_err)?),
            }
        };
        object.insert(column.name().to_string(), value);
    }
    Ok(JsonValue::Object(object))
}

fn encode_postgres_row(row: &PgRow) -> Result<JsonValue, StateError> {
    let mut object = JsonMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(index).map_err(db_err)?;
        let value = if raw.is_null() {
            JsonValue::Null
        } else {
            encode_postgres_value(row, index, raw.type_info().name())?
        };
        object.insert(column.name().to_string(), value);
    }
    Ok(JsonValue::Object(object))
}

fn encode_postgres_value(row: &PgRow, index: usize, kind: &str) -> Result<JsonValue, StateError> {
    Ok(match kind.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => JsonValue::Bool(row.try_get(index).map_err(db_err)?),
        "INT2" | "SMALLINT" => JsonValue::from(row.try_get::<i16, _>(index).map_err(db_err)?),
        "INT4" | "INTEGER" => JsonValue::from(row.try_get::<i32, _>(index).map_err(db_err)?),
        "INT8" | "BIGINT" => JsonValue::from(row.try_get::<i64, _>(index).map_err(db_err)?),
        "FLOAT4" | "REAL" => JsonValue::from(row.try_get::<f32, _>(index).map_err(db_err)? as f64),
        "FLOAT8" | "DOUBLE PRECISION" => {
            JsonValue::from(row.try_get::<f64, _>(index).map_err(db_err)?)
        }
        "BYTEA" => encode_blob(&row.try_get::<Vec<u8>, _>(index).map_err(db_err)?),
        "JSON" | "JSONB" => row.try_get::<JsonValue, _>(index).map_err(db_err)?,
        _ => JsonValue::String(row.try_get::<String, _>(index).map_err(db_err)?),
    })
}

fn encode_blob(bytes: &[u8]) -> JsonValue {
    serde_json::json!({
        BLOB_MARKER_TYPE: "blob",
        BLOB_MARKER_BASE64: STANDARD.encode(bytes),
    })
}

async fn sqlite_table_columns(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
) -> Result<Vec<String>, StateError> {
    let query = format!("PRAGMA table_info({})", quote_identifier(table));
    sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?
        .into_iter()
        .map(|row| row.try_get("name").map_err(db_err))
        .collect()
}

struct SqliteImportColumn {
    name: String,
    nullable: bool,
    has_default: bool,
}

async fn sqlite_import_column_info(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
) -> Result<Vec<SqliteImportColumn>, StateError> {
    let query = format!("PRAGMA table_info({})", quote_identifier(table));
    sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?
        .into_iter()
        .map(|row| {
            Ok(SqliteImportColumn {
                name: row.try_get("name").map_err(db_err)?,
                nullable: row.try_get::<i64, _>("notnull").map_err(db_err)? == 0,
                has_default: row
                    .try_get::<Option<String>, _>("dflt_value")
                    .map_err(db_err)?
                    .is_some(),
            })
        })
        .collect()
}

async fn postgres_table_columns(
    conn: &mut sqlx::PgConnection,
    table: &str,
) -> Result<Vec<String>, StateError> {
    sqlx::query(
        "SELECT column_name FROM information_schema.columns
          WHERE table_schema = current_schema() AND table_name = $1
          ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(conn)
    .await
    .map_err(db_err)?
    .into_iter()
    .map(|row| row.try_get("column_name").map_err(db_err))
    .collect()
}

async fn sqlite_row_order(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
) -> Result<String, StateError> {
    let query = format!("PRAGMA table_info({})", quote_identifier(table));
    let rows = sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
        .fetch_all(conn)
        .await
        .map_err(db_err)?;
    let mut primary = rows
        .iter()
        .filter_map(|row| {
            let position = row.try_get::<i64, _>("pk").ok()?;
            let name = row.try_get::<String, _>("name").ok()?;
            (position > 0).then_some((position, name))
        })
        .collect::<Vec<_>>();
    primary.sort_by_key(|(position, _)| *position);
    if primary.is_empty() {
        Ok("rowid".into())
    } else {
        Ok(primary
            .into_iter()
            .map(|(_, name)| quote_identifier(&name))
            .collect::<Vec<_>>()
            .join(", "))
    }
}

async fn postgres_row_order(
    conn: &mut sqlx::PgConnection,
    table: &str,
) -> Result<String, StateError> {
    let rows = sqlx::query(
        "SELECT kcu.column_name
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = current_schema()
            AND tc.table_name = $1
          ORDER BY kcu.ordinal_position",
    )
    .bind(table)
    .fetch_all(conn)
    .await
    .map_err(db_err)?;
    let columns = rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("column_name").ok())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        Ok("ctid".into())
    } else {
        Ok(columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "))
    }
}

async fn ordered_sqlite_tables(
    conn: &mut sqlx::SqliteConnection,
    actual: &BTreeSet<String>,
    classes: &[BackupTableClassification],
) -> Result<Vec<String>, StateError> {
    let wanted = catalog_tables(classes)
        .intersection(actual)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut dependencies = Vec::new();
    for table in &wanted {
        let query = format!("PRAGMA foreign_key_list({})", quote_identifier(table));
        for row in sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_all(&mut *conn)
            .await
            .map_err(db_err)?
        {
            let parent: String = row.try_get("table").map_err(db_err)?;
            if wanted.contains(&parent) && parent != *table {
                dependencies.push((parent, table.clone()));
            }
        }
    }
    topological_order(wanted, dependencies)
}

async fn ordered_postgres_tables(
    executor: &mut sqlx::PgConnection,
    actual: &BTreeSet<String>,
    classes: &[BackupTableClassification],
) -> Result<Vec<String>, StateError> {
    let wanted = catalog_tables(classes)
        .intersection(actual)
        .cloned()
        .collect::<BTreeSet<_>>();
    let rows = sqlx::query(
        "SELECT child.relname AS child, parent.relname AS parent
           FROM pg_constraint con
           JOIN pg_class child ON child.oid = con.conrelid
           JOIN pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
           JOIN pg_class parent ON parent.oid = con.confrelid
           JOIN pg_namespace parent_namespace ON parent_namespace.oid = parent.relnamespace
          WHERE con.contype = 'f'
            AND child_namespace.nspname = current_schema()
            AND parent_namespace.nspname = current_schema()",
    )
    .fetch_all(executor)
    .await
    .map_err(db_err)?;
    let dependencies = rows
        .into_iter()
        .filter_map(|row| {
            let parent = row.try_get::<String, _>("parent").ok()?;
            let child = row.try_get::<String, _>("child").ok()?;
            (parent != child && wanted.contains(&parent) && wanted.contains(&child))
                .then_some((parent, child))
        })
        .collect();
    topological_order(wanted, dependencies)
}

fn topological_order(
    tables: BTreeSet<String>,
    dependencies: Vec<(String, String)>,
) -> Result<Vec<String>, StateError> {
    let mut incoming = tables
        .iter()
        .map(|table| (table.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = tables
        .iter()
        .map(|table| (table.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (parent, child) in dependencies {
        if outgoing
            .get_mut(&parent)
            .is_some_and(|set| set.insert(child.clone()))
        {
            *incoming.get_mut(&child).expect("catalog child") += 1;
        }
    }
    let mut ready = incoming
        .iter()
        .filter_map(|(table, count)| (*count == 0).then_some(table.clone()))
        .collect::<VecDeque<_>>();
    let mut ordered = Vec::new();
    while let Some(table) = ready.pop_front() {
        ordered.push(table.clone());
        for child in outgoing.get(&table).cloned().unwrap_or_default() {
            let count = incoming.get_mut(&child).expect("catalog child");
            *count -= 1;
            if *count == 0 {
                let position = ready
                    .iter()
                    .position(|candidate| candidate > &child)
                    .unwrap_or(ready.len());
                ready.insert(position, child);
            }
        }
    }
    if ordered.len() == tables.len() {
        Ok(ordered)
    } else {
        Err(StateError::Database(
            "backup catalog dependencies contain a cycle".into(),
        ))
    }
}

// Import is intentionally in this module so export and restore share the same
// typed row contract.
async fn import_sqlite(
    pool: sqlx::SqlitePool,
    tables_dir: &Path,
    expected: &BTreeMap<String, TablePartMetadata>,
    allow_older_catalog: bool,
    source_schema_version: i64,
) -> Result<(), StateError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    let actual = validate_sqlite_catalog(&mut conn).await?;
    let clear = ordered_sqlite_tables(
        &mut conn,
        &actual,
        &[
            BackupTableClassification::Export,
            BackupTableClassification::ResetOnRestore,
            BackupTableClassification::Rebuild,
        ],
    )
    .await?;
    let export =
        ordered_sqlite_tables(&mut conn, &actual, &[BackupTableClassification::Export]).await?;
    let import = import_table_order(expected, &export, allow_older_catalog)?;
    let restored = restored_tables(expected, &import);
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let result = async {
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        for table in clear.iter().rev() {
            let query = format!("DELETE FROM {}", quote_identifier(table));
            sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
        for table in &import {
            import_sqlite_table(
                &mut conn,
                table,
                &tables_dir.join(format!("{table}.ndjson")),
                expected.get(table).expect("validated import table"),
                allow_older_catalog,
            )
            .await?;
        }
        sqlx::query("INSERT INTO script_output_state (singleton, next_seq) VALUES (1, 0)")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
        if source_schema_version < EGRESS_CATALOG_SCHEMA_VERSION
            && !expected.contains_key("egress_interfaces")
        {
            sqlx::query(SYSTEM_EGRESS_SEED)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
        let system_egress: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM egress_interfaces WHERE id = 0")
                .fetch_one(&mut *conn)
                .await
                .map_err(db_err)?;
        if system_egress != 1 {
            return Err(StateError::Database(
                "restored networking configuration is missing the System egress".into(),
            ));
        }
        repair_sqlite_sequences(&mut conn).await?;
        let violations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pragma_foreign_key_check")
            .fetch_one(&mut *conn)
            .await
            .map_err(db_err)?;
        if violations != 0 {
            return Err(StateError::Database(format!(
                "restored database has {violations} foreign-key violations"
            )));
        }
        validate_sqlite_counts(&mut conn, &restored).await?;
        move_older_bandwidth_cap(
            &mut SqlConn::Sqlite(&mut conn),
            source_schema_version,
            expected,
        )
        .await?;
        move_older_script_wiring(
            &mut SqlConn::Sqlite(&mut conn),
            source_schema_version,
            expected,
        )
        .await
    }
    .await;
    match result {
        Ok(()) => sqlx::query("COMMIT")
            .execute(&mut *conn)
            .await
            .map(|_| ())
            .map_err(db_err),
        Err(error) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(error)
        }
    }
}

async fn import_postgres(
    pool: sqlx::PgPool,
    tables_dir: &Path,
    expected: &BTreeMap<String, TablePartMetadata>,
    allow_older_catalog: bool,
    source_schema_version: i64,
) -> Result<(), StateError> {
    let mut conn = pool.acquire().await.map_err(db_err)?;
    let actual = validate_postgres_catalog(&mut conn).await?;
    let mut tx = conn.begin().await.map_err(db_err)?;
    let clear = ordered_postgres_tables(
        &mut tx,
        &actual,
        &[
            BackupTableClassification::Export,
            BackupTableClassification::ResetOnRestore,
            BackupTableClassification::Rebuild,
        ],
    )
    .await?;
    let export =
        ordered_postgres_tables(&mut tx, &actual, &[BackupTableClassification::Export]).await?;
    let import = import_table_order(expected, &export, allow_older_catalog)?;
    let restored = restored_tables(expected, &import);
    for table in clear.iter().rev() {
        let query = format!("DELETE FROM {}", quote_identifier(table));
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    for table in &import {
        import_postgres_table(
            &mut tx,
            table,
            &tables_dir.join(format!("{table}.ndjson")),
            expected.get(table).expect("validated import table"),
            allow_older_catalog,
        )
        .await?;
    }
    sqlx::query("INSERT INTO script_output_state (singleton, next_seq) VALUES (1, 0)")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    if source_schema_version < EGRESS_CATALOG_SCHEMA_VERSION
        && !expected.contains_key("egress_interfaces")
    {
        sqlx::query(SYSTEM_EGRESS_SEED)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    let system_egress: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM egress_interfaces WHERE id = 0")
            .fetch_one(&mut *tx)
            .await
            .map_err(db_err)?;
    if system_egress != 1 {
        return Err(StateError::Database(
            "restored networking configuration is missing the System egress".into(),
        ));
    }
    validate_postgres_counts(&mut tx, &restored).await?;
    repair_postgres_sequences(&mut tx).await?;
    move_older_bandwidth_cap(
        &mut SqlConn::Postgres(&mut tx),
        source_schema_version,
        expected,
    )
    .await?;
    move_older_script_wiring(
        &mut SqlConn::Postgres(&mut tx),
        source_schema_version,
        expected,
    )
    .await?;
    tx.commit().await.map_err(db_err)
}

/// A bundle written before egresses kept download quotas can carry the
/// bandwidth cap they replaced. No migration runs over a restore, so the step
/// that moves the cap onto the System egress on an upgrade is run here instead.
async fn move_older_bandwidth_cap(
    conn: &mut SqlConn<'_>,
    source_schema_version: i64,
    expected: &BTreeMap<String, TablePartMetadata>,
) -> Result<(), StateError> {
    if source_schema_version >= egress_quotas_v53::SCHEMA_VERSION
        || expected.contains_key("egress_download_usage")
    {
        return Ok(());
    }
    egress_quotas_v53::move_isp_cap_to_system_egress(conn).await
}

/// A bundle written before there were script instances carries the wiring
/// they replaced. No migration runs over a restore, so the step that moves
/// that wiring across on an upgrade is run here instead.
async fn move_older_script_wiring(
    conn: &mut SqlConn<'_>,
    source_schema_version: i64,
    expected: &BTreeMap<String, TablePartMetadata>,
) -> Result<(), StateError> {
    if source_schema_version >= script_instances_v55::SCHEMA_VERSION
        || expected.contains_key(script_instances_v55::INSTANCES_TABLE)
    {
        return Ok(());
    }
    script_instances_v55::move_script_wiring_to_instances(conn).await
}

const EGRESS_CATALOG_SCHEMA_VERSION: i64 = 53;
const SYSTEM_EGRESS_SEED: &str = "INSERT INTO egress_interfaces (id, name, binding_kind, binding_value, enabled, max_download_speed, created_at, updated_at) VALUES (0, 'System', 'system', NULL, 1, 0, 0, 0)";

/// Chooses the tables to restore, in dependency order. A bundle from the
/// current schema must carry exactly the export catalog. A bundle from an older
/// schema restores the tables both sides know: tables it predates stay empty,
/// and tables retired since it was written are left out.
fn import_table_order(
    expected: &BTreeMap<String, TablePartMetadata>,
    export: &[String],
    older_source: bool,
) -> Result<Vec<String>, StateError> {
    if older_source {
        return Ok(export
            .iter()
            .filter(|table| expected.contains_key(*table))
            .cloned()
            .collect());
    }
    validate_manifest_tables(expected, export)?;
    Ok(export.to_vec())
}

fn restored_tables(
    expected: &BTreeMap<String, TablePartMetadata>,
    import: &[String],
) -> BTreeMap<String, TablePartMetadata> {
    import
        .iter()
        .filter_map(|table| Some((table.clone(), expected.get(table)?.clone())))
        .collect()
}

fn validate_manifest_tables(
    expected: &BTreeMap<String, TablePartMetadata>,
    export: &[String],
) -> Result<(), StateError> {
    let expected = expected.keys().cloned().collect::<BTreeSet<_>>();
    let actual = export.iter().cloned().collect::<BTreeSet<_>>();
    let missing = actual.difference(&expected).cloned().collect::<Vec<_>>();
    let unexpected = expected.difference(&actual).cloned().collect::<Vec<_>>();
    if unexpected.is_empty() && missing.is_empty() {
        return Ok(());
    }
    Err(StateError::Database(format!(
        "backup table set does not match restore catalog: missing [{}], unexpected [{}]",
        missing.join(", "),
        unexpected.join(", ")
    )))
}

async fn import_sqlite_table(
    conn: &mut sqlx::SqliteConnection,
    table: &str,
    path: &Path,
    source: &TablePartMetadata,
    allow_older_catalog: bool,
) -> Result<(), StateError> {
    let target_columns = sqlite_import_column_info(conn, table).await?;
    let target = target_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let unknown = source
        .columns
        .iter()
        .filter(|column| !target.contains(*column))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(StateError::Database(format!(
            "backup table {table} contains unsupported source columns: {}",
            unknown.join(", ")
        )));
    }
    let source_columns = source.columns.iter().collect::<BTreeSet<_>>();
    let missing = target_columns
        .iter()
        .filter(|column| !source_columns.contains(&column.name))
        .filter(|column| !allow_older_catalog || (!column.nullable && !column.has_default))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(StateError::Database(format!(
            "backup table {table} is missing required target columns: {}",
            missing.join(", ")
        )));
    }
    for row in read_ndjson_rows(path)? {
        let (line_number, object) = row?;
        let columns = object
            .keys()
            .filter(|column| target.contains(*column))
            .cloned()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            continue;
        }
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_identifier(table),
            columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            std::iter::repeat_n("?", columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut query: Query<'_, Sqlite, SqliteArguments> =
            sqlx::query(sqlx::AssertSqlSafe(query.as_str()));
        for column in &columns {
            query = bind_sqlite_value(query, object.get(column).unwrap_or(&JsonValue::Null))?;
        }
        query.execute(&mut *conn).await.map_err(|error| {
            StateError::Database(format!(
                "failed to import backup row {table}:{}: {error}",
                line_number + 1
            ))
        })?;
    }
    Ok(())
}

async fn import_postgres_table(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    table: &str,
    path: &Path,
    source: &TablePartMetadata,
    allow_older_catalog: bool,
) -> Result<(), StateError> {
    let target = postgres_column_info(tx, table).await?;
    let target_names = target
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let unknown = source
        .columns
        .iter()
        .filter(|column| !target_names.contains(column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        return Err(StateError::Database(format!(
            "backup table {table} contains unsupported source columns: {}",
            unknown.join(", ")
        )));
    }
    let source_columns = source
        .columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing = target
        .iter()
        .filter(|column| !source_columns.contains(column.name.as_str()))
        .filter(|column| !allow_older_catalog || (!column.nullable && !column.has_default))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(StateError::Database(format!(
            "backup table {table} is missing required target columns: {}",
            missing.join(", ")
        )));
    }
    for row in read_ndjson_rows(path)? {
        let (line_number, object) = row?;
        let columns = target
            .iter()
            .filter(|column| object.contains_key(&column.name))
            .collect::<Vec<_>>();
        if columns.is_empty() {
            continue;
        }
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_identifier(table),
            columns
                .iter()
                .map(|column| quote_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", "),
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| format!("${}{}", index + 1, postgres_cast(&column.kind)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut query: Query<'_, Postgres, PgArguments> =
            sqlx::query(sqlx::AssertSqlSafe(query.as_str()));
        for column in columns {
            query = bind_postgres_value(
                query,
                &column.kind,
                object.get(&column.name).unwrap_or(&JsonValue::Null),
            )?;
        }
        query.execute(&mut **tx).await.map_err(|error| {
            StateError::Database(format!(
                "failed to import backup row {table}:{}: {error}",
                line_number + 1
            ))
        })?;
    }
    Ok(())
}

fn read_ndjson_rows(path: &Path) -> Result<NdjsonRows, StateError> {
    let file = File::open(path).map_err(db_err)?;
    Ok(NdjsonRows {
        reader: BufReader::new(file),
        display: path.display().to_string(),
        line_number: 0,
        line: Vec::with_capacity(16 * 1024),
        line_limit: backup_payload_limit(),
    })
}

fn backup_payload_limit() -> usize {
    usize::try_from(
        RuntimeSecurityConfig::from_env_or_default_for_tests().backup_upload_limit_bytes,
    )
    .unwrap_or(usize::MAX)
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    limit: usize,
) -> Result<usize, StateError> {
    loop {
        let available = reader.fill_buf().map_err(db_err)?;
        if available.is_empty() {
            return Ok(line.len());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        let content = newline.map_or(take, |index| index);
        if line.len().saturating_add(content) > limit {
            return Err(StateError::Database(format!(
                "backup row exceeds {limit} bytes"
            )));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

fn bind_sqlite_value<'q>(
    query: Query<'q, Sqlite, SqliteArguments>,
    value: &'q JsonValue,
) -> Result<Query<'q, Sqlite, SqliteArguments>, StateError> {
    Ok(match value {
        JsonValue::Null => query.bind(None::<String>),
        JsonValue::Bool(value) => query.bind(i64::from(*value)),
        JsonValue::Number(value) => {
            if let Some(integer) = value.as_i64() {
                query.bind(integer)
            } else {
                query.bind(value.as_f64())
            }
        }
        JsonValue::String(value) => query.bind(value),
        JsonValue::Object(_) if is_blob(value) => query.bind(blob_bytes(value)?),
        JsonValue::Array(_) | JsonValue::Object(_) => query.bind(value.to_string()),
    })
}

fn bind_postgres_value<'q>(
    query: Query<'q, Postgres, PgArguments>,
    kind: &str,
    value: &'q JsonValue,
) -> Result<Query<'q, Postgres, PgArguments>, StateError> {
    if kind == "bytea" {
        return Ok(match value {
            JsonValue::Null => query.bind(None::<Vec<u8>>),
            _ => query.bind(blob_bytes(value)?),
        });
    }
    if matches!(kind, "json" | "jsonb") {
        return Ok(match value {
            JsonValue::Null => query.bind(None::<sqlx::types::Json<JsonValue>>),
            value => query.bind(sqlx::types::Json(value.clone())),
        });
    }
    Ok(match value {
        JsonValue::Null => query.bind(None::<String>),
        JsonValue::Bool(value) => query.bind(value.to_string()),
        JsonValue::Number(value) => query.bind(value.to_string()),
        JsonValue::String(value) => query.bind(value),
        JsonValue::Array(_) | JsonValue::Object(_) => query.bind(value.to_string()),
    })
}

fn is_blob(value: &JsonValue) -> bool {
    value
        .as_object()
        .and_then(|object| object.get(BLOB_MARKER_TYPE))
        .and_then(JsonValue::as_str)
        == Some("blob")
}

fn blob_bytes(value: &JsonValue) -> Result<Vec<u8>, StateError> {
    let encoded = value
        .as_object()
        .and_then(|object| object.get(BLOB_MARKER_BASE64))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| StateError::Database("backup blob is missing base64 bytes".into()))?;
    STANDARD.decode(encoded).map_err(db_err)
}

#[derive(Clone)]
struct PostgresColumn {
    name: String,
    kind: String,
    nullable: bool,
    has_default: bool,
}

async fn postgres_column_info(
    conn: &mut sqlx::PgConnection,
    table: &str,
) -> Result<Vec<PostgresColumn>, StateError> {
    sqlx::query(
        "SELECT column_name, udt_name,
                is_nullable = 'YES' AS nullable,
                column_default IS NOT NULL OR is_identity = 'YES' AS has_default
           FROM information_schema.columns
          WHERE table_schema = current_schema() AND table_name = $1
          ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(conn)
    .await
    .map_err(db_err)?
    .into_iter()
    .map(|row| {
        Ok(PostgresColumn {
            name: row.try_get("column_name").map_err(db_err)?,
            kind: row.try_get("udt_name").map_err(db_err)?,
            nullable: row.try_get("nullable").map_err(db_err)?,
            has_default: row.try_get("has_default").map_err(db_err)?,
        })
    })
    .collect()
}

fn postgres_cast(kind: &str) -> &'static str {
    match kind {
        "bool" => "::boolean",
        "int2" => "::smallint",
        "int4" => "::integer",
        "int8" => "::bigint",
        "float4" => "::real",
        "float8" => "::double precision",
        "numeric" => "::numeric",
        "timestamp" => "::timestamp",
        "timestamptz" => "::timestamptz",
        "date" => "::date",
        "json" => "::json",
        "jsonb" => "::jsonb",
        _ => "",
    }
}

async fn repair_sqlite_sequences(conn: &mut sqlx::SqliteConnection) -> Result<(), StateError> {
    let has_sequence_table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
          WHERE type = 'table' AND name = 'sqlite_sequence'",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(db_err)?;
    if has_sequence_table != 0 {
        sqlx::query("DELETE FROM sqlite_sequence")
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn validate_sqlite_counts(
    conn: &mut sqlx::SqliteConnection,
    expected: &BTreeMap<String, TablePartMetadata>,
) -> Result<(), StateError> {
    for (table, metadata) in expected {
        let query = format!("SELECT COUNT(*) FROM {}", quote_identifier(table));
        let actual: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(&mut *conn)
            .await
            .map_err(db_err)?;
        if actual as u64 != metadata.rows {
            return Err(StateError::Database(format!(
                "restored table {table} row count mismatch: expected {}, got {actual}",
                metadata.rows
            )));
        }
    }
    Ok(())
}

async fn validate_postgres_counts(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    expected: &BTreeMap<String, TablePartMetadata>,
) -> Result<(), StateError> {
    for (table, metadata) in expected {
        let query = format!("SELECT COUNT(*) FROM {}", quote_identifier(table));
        let actual: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(&mut **tx)
            .await
            .map_err(db_err)?;
        if actual as u64 != metadata.rows {
            return Err(StateError::Database(format!(
                "restored table {table} row count mismatch: expected {}, got {actual}",
                metadata.rows
            )));
        }
    }
    Ok(())
}

async fn repair_postgres_sequences(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<(), StateError> {
    let rows = sqlx::query(
        "SELECT ns.nspname AS sequence_schema, seq.relname AS sequence_name,
                tbl.relname AS table_name, att.attname AS column_name
           FROM pg_class seq
           JOIN pg_namespace ns ON ns.oid = seq.relnamespace
           JOIN pg_depend dep ON dep.objid = seq.oid AND dep.deptype = 'a'
           JOIN pg_class tbl ON tbl.oid = dep.refobjid
           JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = dep.refobjsubid
          WHERE seq.relkind = 'S' AND ns.nspname = current_schema()",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(db_err)?;
    for row in rows {
        let schema: String = row.try_get("sequence_schema").map_err(db_err)?;
        let sequence: String = row.try_get("sequence_name").map_err(db_err)?;
        let table: String = row.try_get("table_name").map_err(db_err)?;
        let column: String = row.try_get("column_name").map_err(db_err)?;
        let qualified = format!(
            "{}.{}",
            quote_identifier(&schema),
            quote_identifier(&sequence)
        );
        let query = format!(
            "SELECT setval(
                $1::regclass,
                GREATEST(COALESCE((SELECT MAX({}) FROM {}), 0), 1),
                COALESCE((SELECT MAX({}) FROM {}), 0) > 0
             )",
            quote_identifier(&column),
            quote_identifier(&table),
            quote_identifier(&column),
            quote_identifier(&table),
        );
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .bind(qualified)
            .execute(&mut **tx)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

pub(crate) fn verify_table_parts(
    root: &Path,
    expected: &BTreeMap<String, TablePartMetadata>,
) -> Result<(), StateError> {
    for (table, metadata) in expected {
        let declared_columns = metadata.columns.iter().cloned().collect::<BTreeSet<_>>();
        if declared_columns.len() != metadata.columns.len()
            || metadata.checksum.len() != 64
            || !metadata
                .checksum
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(StateError::Database(format!(
                "backup table {table} has invalid part metadata"
            )));
        }
        let path = root.join("tables").join(format!("{table}.ndjson"));
        let mut reader = BufReader::new(File::open(&path).map_err(db_err)?);
        let mut hasher = blake3::Hasher::new();
        let mut rows = 0_u64;
        let mut buffer = Vec::new();
        let line_limit = backup_payload_limit();
        loop {
            buffer.clear();
            let read = read_bounded_line(&mut reader, &mut buffer, line_limit)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer);
            if buffer.iter().any(|byte| !byte.is_ascii_whitespace()) {
                let value = serde_json::from_slice::<JsonValue>(&buffer).map_err(db_err)?;
                let object = value.as_object().ok_or_else(|| {
                    StateError::Database(format!("backup table {table} row is not an object"))
                })?;
                let row_columns = object.keys().cloned().collect::<BTreeSet<_>>();
                if row_columns != declared_columns {
                    return Err(StateError::Database(format!(
                        "backup table {table} row columns do not match its manifest"
                    )));
                }
                rows += 1;
            }
        }
        let checksum = hasher.finalize().to_hex().to_string();
        if checksum != metadata.checksum || rows != metadata.rows {
            return Err(StateError::Database(format!(
                "backup table {table} failed checksum or row-count validation"
            )));
        }
    }
    Ok(())
}

pub(crate) fn read_table_objects(
    root: &Path,
    table: &str,
) -> Result<Vec<JsonMap<String, JsonValue>>, StateError> {
    let mut rows = Vec::new();
    visit_table_objects(root, table, |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
}

pub(crate) fn visit_table_objects<F>(
    root: &Path,
    table: &str,
    mut visit: F,
) -> Result<(), StateError>
where
    F: FnMut(JsonMap<String, JsonValue>) -> Result<(), StateError>,
{
    let path = root.join("tables").join(format!("{table}.ndjson"));
    if !path.exists() {
        return Ok(());
    }
    for row in read_ndjson_rows(&path)? {
        let (_, row) = row?;
        visit(row)?;
    }
    Ok(())
}

pub(crate) fn rewrite_table_objects<F>(
    root: &Path,
    table: &str,
    mut rewrite: F,
) -> Result<TablePartMetadata, StateError>
where
    F: FnMut(&mut JsonMap<String, JsonValue>) -> Result<(), StateError>,
{
    let path = root.join("tables").join(format!("{table}.ndjson"));
    let parent = path
        .parent()
        .ok_or_else(|| StateError::Database("backup table has no parent directory".into()))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent).map_err(db_err)?;
    let mut writer = BufWriter::new(staged.as_file_mut());
    let mut hasher = blake3::Hasher::new();
    let mut line = Vec::new();
    let mut columns = BTreeSet::new();
    let mut row_count = 0_u64;
    for row in read_ndjson_rows(&path)? {
        let (_, mut row) = row?;
        rewrite(&mut row)?;
        columns.extend(row.keys().cloned());
        line.clear();
        serde_json::to_writer(&mut line, &row).map_err(db_err)?;
        line.push(b'\n');
        writer.write_all(&line).map_err(db_err)?;
        hasher.update(&line);
        row_count += 1;
    }
    writer.flush().map_err(db_err)?;
    drop(writer);
    staged.as_file().sync_all().map_err(db_err)?;
    staged.persist(&path).map_err(db_err)?;
    Ok(TablePartMetadata {
        rows: row_count,
        columns: columns.into_iter().collect(),
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

pub(crate) fn append_table_objects(
    root: &Path,
    table: &str,
    rows: &[JsonMap<String, JsonValue>],
    metadata: &TablePartMetadata,
) -> Result<TablePartMetadata, StateError> {
    if rows.is_empty() {
        return Ok(metadata.clone());
    }
    let declared_columns = metadata.columns.iter().cloned().collect::<BTreeSet<_>>();
    for row in rows {
        if row.keys().cloned().collect::<BTreeSet<_>>() != declared_columns {
            return Err(StateError::Database(format!(
                "appended backup row for {table} does not match its source columns"
            )));
        }
    }
    let path = root.join("tables").join(format!("{table}.ndjson"));
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .map_err(db_err)?;
    for row in rows {
        serde_json::to_writer(&mut file, row).map_err(db_err)?;
        file.write_all(b"\n").map_err(db_err)?;
    }
    file.flush().map_err(db_err)?;
    file.sync_all().map_err(db_err)?;

    let mut source = File::open(&path).map_err(db_err)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut source, &mut hasher).map_err(db_err)?;
    Ok(TablePartMetadata {
        rows: metadata
            .rows
            .checked_add(u64::try_from(rows.len()).map_err(db_err)?)
            .ok_or_else(|| StateError::Database("backup table row count overflow".into()))?,
        columns: metadata.columns.clone(),
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

pub(crate) async fn validate_legacy_encryption_key(
    path: &Path,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<(), StateError> {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false);
    let mut conn = sqlx::SqliteConnection::connect_with(&options)
        .await
        .map_err(db_err)?;
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    )
    .fetch_all(&mut conn)
    .await
    .map_err(db_err)?;
    for table in tables {
        if is_engine_internal_table(&table) {
            continue;
        }
        let columns = sqlite_table_columns(&mut conn, &table).await?;
        for column in columns {
            let query = format!(
                "SELECT CAST({column} AS TEXT) FROM {table}
                  WHERE typeof({column}) = 'text' AND {column} LIKE 'enc:v1:%'",
                column = quote_identifier(&column),
                table = quote_identifier(&table),
            );
            let values = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(query.as_str()))
                .fetch_all(&mut conn)
                .await
                .map_err(db_err)?;
            let key = if values.is_empty() {
                continue;
            } else {
                key.ok_or_else(|| {
                    StateError::Database(
                        "legacy backup contains encrypted values but the target has no encryption key"
                            .into(),
                    )
                })?
            };
            for value in values {
                crate::persistence::encryption::decrypt_value(key, &value)
                    .map_err(StateError::Database)?;
            }
        }
    }
    Ok(())
}

/// Export an existing schema before opening the application database or running migrations.
#[cfg(test)]
pub(super) async fn export_before_migrations(
    target: &crate::persistence::database_target::DatabaseTarget,
) -> Result<LogicalBackupExport, StateError> {
    export_before_migrations_cancellable(target, super::archive::BackupCancellation::new()).await
}

pub(super) async fn export_before_migrations_cancellable(
    target: &crate::persistence::database_target::DatabaseTarget,
    cancellation: super::archive::BackupCancellation,
) -> Result<LogicalBackupExport, StateError> {
    use crate::persistence::database_target::DatabaseTarget;
    let staging = super::create_backup_temp_dir().map_err(db_err)?;
    let tables_dir = staging.path().join("tables");
    std::fs::create_dir_all(&tables_dir).map_err(db_err)?;
    let (source_engine, schema_version, tables) = match target {
        DatabaseTarget::PostgresUrl(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await
                .map_err(db_err)?;
            let result = async {
                let version = sqlx::query_scalar::<_, i64>("SELECT version FROM schema_version")
                    .fetch_one(&pool)
                    .await
                    .map_err(db_err)?;
                let tables =
                    export_postgres(pool.clone(), &tables_dir, true, &cancellation).await?;
                Ok::<_, StateError>(("postgres", version, tables))
            }
            .await;
            pool.close().await;
            result?
        }
        target => {
            let path = target
                .sqlite_path()?
                .ok_or_else(|| StateError::Database("SQLite backup has no path".into()))?;
            let options = SqliteConnectOptions::new()
                .filename(&path)
                .read_only(true)
                .create_if_missing(false);
            let mut connection = sqlx::SqliteConnection::connect_with(&options)
                .await
                .map_err(db_err)?;
            let version = sqlx::query_scalar::<_, i64>("SELECT version FROM schema_version")
                .fetch_one(&mut connection)
                .await
                .map_err(db_err)?;
            connection.close().await.map_err(db_err)?;
            (
                "sqlite",
                version,
                export_sqlite(&path, &tables_dir, true, &cancellation).await?,
            )
        }
    };
    Ok(LogicalBackupExport {
        staging,
        source_engine: source_engine.into(),
        schema_version,
        tables,
    })
}

#[cfg(test)]
mod logical_reader_tests {
    use super::*;

    #[tokio::test]
    async fn pre_migration_export_preserves_unclassified_tables_and_checks_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut conn = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE schema_version (version INTEGER); INSERT INTO schema_version VALUES (1); CREATE TABLE retired_settings (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO retired_settings VALUES (7, 'retained');")
            .execute(&mut conn).await.unwrap();
        assert!(validate_sqlite_catalog(&mut conn).await.is_err());
        conn.close().await.unwrap();
        let target = crate::persistence::database_target::DatabaseTarget::SqlitePath(path.clone());
        let export = export_before_migrations(&target).await.unwrap();
        assert_eq!(export.tables["retired_settings"].rows, 1);
        assert_eq!(
            read_table_objects(export.staging.path(), "retired_settings").unwrap()[0]["value"],
            "retained"
        );

        let tables = tempfile::tempdir().unwrap();
        let mut conn = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let cancellation = super::super::archive::BackupCancellation::new();
        export_sqlite_table(&mut conn, "schema_version", tables.path(), &cancellation)
            .await
            .unwrap();
        cancellation.cancel();
        let error =
            export_sqlite_table(&mut conn, "retired_settings", tables.path(), &cancellation)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(!tables.path().join("retired_settings.ndjson").exists());
    }

    #[test]
    fn restore_manifest_requires_the_complete_export_catalog() {
        let expected = BTreeMap::from([("settings".to_string(), TablePartMetadata::default())]);
        let export = vec!["settings".to_string(), "servers".to_string()];

        let error = validate_manifest_tables(&expected, &export).unwrap_err();

        assert!(error.to_string().contains("missing [servers]"));
    }

    #[test]
    fn older_bundle_restores_despite_retired_and_newer_tables() {
        let source = Database::open_in_memory().unwrap();
        let mut archive = source.export_logical_backup().unwrap();
        let tables = archive.staging.path().join("tables");
        std::fs::write(
            tables.join("retired_settings.ndjson"),
            b"{\"id\":7,\"value\":\"retained\"}\n",
        )
        .unwrap();
        archive.tables.insert(
            "retired_settings".into(),
            TablePartMetadata {
                rows: 1,
                columns: vec!["id".into(), "value".into()],
                checksum: String::new(),
            },
        );
        // The source predates this table, so its bundle has no part for it.
        archive.tables.remove("rss_seen_items");

        let target = Database::open_in_memory().unwrap();
        let error = target
            .import_logical_backup(&tables, &archive.tables, archive.schema_version)
            .unwrap_err();
        assert!(error.to_string().contains("unexpected [retired_settings]"));

        target
            .import_logical_backup(&tables, &archive.tables, archive.schema_version - 1)
            .unwrap();
        assert_eq!(target.list_egress_interfaces().unwrap().len(), 1);
    }

    #[test]
    fn a_bundle_from_before_instances_restores_its_script_wiring_as_instances() {
        use crate::post_processing::instances::InstanceTrigger;
        use crate::post_processing::model::GlobalScriptsRun;
        use crate::schema_migrations::script_instances_v55::SCHEMA_VERSION;

        let root = tempfile::tempdir().unwrap();
        let scripts = std::fs::canonicalize(root.path()).unwrap();
        for name in ["post.sh", "tv.sh"] {
            std::fs::write(
                scripts.join(name),
                "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\n",
            )
            .unwrap();
        }
        // The wiring as the build before instances saved it.
        let source = Database::open_in_memory().unwrap();
        let sealed = crate::persistence::encryption::encrypt_value(
            source.encryption_key().unwrap(),
            "carried",
        )
        .unwrap();
        for (key, value) in [
            (
                "post_processing.script_directory.v1",
                scripts.to_string_lossy().into_owned(),
            ),
            (
                "post_processing.script_lists.v1",
                serde_json::json!({
                    "global": [{"script": "post.sh"}],
                    "categories": {"tv": [{"script": "tv.sh", "timeoutSeconds": 60}]},
                })
                .to_string(),
            ),
            (
                "post_processing.script_options.v1",
                serde_json::json!({
                    "tv.sh": {"secrets": [{"name": "Token", "ciphertext": sealed.clone()}]},
                })
                .to_string(),
            ),
        ] {
            source.set_setting(key, &value).unwrap();
        }
        let mut archive = source.export_logical_backup().unwrap();
        let tables = archive.staging.path().join("tables");
        // The source predates instances, so its bundle has no part for them.
        for table in [
            "script_instances",
            "script_instance_inputs",
            "script_instance_categories",
            "secrets",
            "feed_scripts",
        ] {
            archive.tables.remove(table);
        }

        // A restore brings the bundle's key along with its values.
        let mut target = Database::open_in_memory().unwrap();
        target.set_encryption_key(source.encryption_key().unwrap().clone());
        target
            .import_logical_backup(&tables, &archive.tables, SCHEMA_VERSION - 1)
            .unwrap();
        assert_eq!(
            target
                .script_instances()
                .unwrap()
                .iter()
                .map(|instance| (
                    instance.script.as_str().to_string(),
                    instance.trigger,
                    instance.categories.clone(),
                    instance.enabled,
                    instance.timeout_seconds,
                ))
                .collect::<Vec<_>>(),
            [
                (
                    "post.sh".to_string(),
                    InstanceTrigger::PostProcessing,
                    vec![],
                    true,
                    None
                ),
                (
                    "tv.sh".to_string(),
                    InstanceTrigger::PostProcessing,
                    vec!["tv".to_string()],
                    true,
                    Some(60)
                ),
            ]
        );
        assert_eq!(
            target
                .post_processing_settings()
                .unwrap()
                .global_scripts_run,
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        );
        for key in [
            "post_processing.script_lists.v1",
            "post_processing.script_options.v1",
        ] {
            assert_eq!(target.get_setting(key).unwrap(), None, "{key}");
        }
        // The saved secret option came across as a named secret the instance
        // links.
        let secrets = target.secrets().unwrap();
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "tv.sh Token");
        let tv = &target.script_instances().unwrap()[1];
        assert_eq!(
            tv.inputs[0]
                .secret
                .as_ref()
                .map(|secret| secret.id.as_str()),
            Some(secrets[0].id.as_str())
        );
        assert!(matches!(
            target.script_instance_run_inputs(&tv.id).unwrap().unwrap()[0].value(),
            crate::post_processing::model::OptionValue::Secret(value)
                if value.expose_for_execution() == "carried"
        ));
    }

    #[test]
    fn named_secrets_and_their_links_round_trip_through_a_bundle() {
        use crate::post_processing::instances::{InstanceTrigger, ScriptInstanceDraft};
        use crate::post_processing::model::{OptionValue, ScriptName};

        let source = Database::open_in_memory().unwrap();
        let token = source.create_secret("Mail token", "round-trip").unwrap();
        let unused = source.create_secret("Spare", "unused").unwrap();
        let instance = source
            .create_script_instance(
                ScriptInstanceDraft::new(
                    ScriptName::new("notify.sh").unwrap(),
                    InstanceTrigger::PostProcessing,
                )
                .input("Host", "mail.example.invalid")
                .secret_input("Token", &token.id)
                .sealed_input("Password", "kept-with-the-instance"),
            )
            .unwrap();
        let archive = source.export_logical_backup().unwrap();
        let tables = archive.staging.path().join("tables");
        assert_eq!(archive.tables["secrets"].rows, 2);
        // The bundle carries the value sealed, never in clear.
        let rows = read_table_objects(archive.staging.path(), "secrets").unwrap();
        assert!(
            rows.iter()
                .all(|row| !serde_json::to_string(row).unwrap().contains("round-trip"))
        );
        // So it does a secret of the instance's own, in the instance's row.
        let rows = read_table_objects(archive.staging.path(), "script_instance_inputs").unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| {
            !serde_json::to_string(row)
                .unwrap()
                .contains("kept-with-the-instance")
        }));
        let own = rows.iter().find(|row| row["name"] == "Password").unwrap();
        assert_eq!(own["value"], "");
        assert!(crate::persistence::encryption::is_encrypted(
            own["sealed_value"].as_str().unwrap()
        ));

        let mut target = Database::open_in_memory().unwrap();
        target.set_encryption_key(source.encryption_key().unwrap().clone());
        target
            .import_logical_backup(&tables, &archive.tables, archive.schema_version)
            .unwrap();
        let restored = target.secrets().unwrap();
        assert_eq!(
            restored
                .iter()
                .map(|secret| (
                    secret.id.as_str(),
                    secret.name.as_str(),
                    secret.used_by.len()
                ))
                .collect::<Vec<_>>(),
            [
                (token.id.as_str(), "Mail token", 1),
                (unused.id.as_str(), "Spare", 0),
            ]
        );
        assert_eq!(
            target.script_instance(&instance.id).unwrap().unwrap(),
            instance
        );
        let run = target
            .script_instance_run_inputs(&instance.id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            run[1].value(),
            OptionValue::Secret(value) if value.expose_for_execution() == "round-trip"
        ));
        // The instance's own secret came back with it, and no named secret
        // was made of it.
        assert!(
            target
                .script_instance(&instance.id)
                .unwrap()
                .unwrap()
                .inputs[2]
                .sealed
        );
        assert!(matches!(
            run[2].value(),
            OptionValue::Secret(value)
                if value.expose_for_execution() == "kept-with-the-instance"
        ));
        target
            .validate_encrypted_credentials(source.encryption_key().unwrap())
            .unwrap();
    }

    #[test]
    fn a_restore_replaces_linked_secrets_already_on_the_target() {
        use crate::post_processing::instances::{InstanceTrigger, ScriptInstanceDraft};
        use crate::post_processing::model::{OptionValue, ScriptName};

        let draft = |script: &str| {
            ScriptInstanceDraft::new(
                ScriptName::new(script).unwrap(),
                InstanceTrigger::PostProcessing,
            )
        };
        let source = Database::open_in_memory().unwrap();
        let token = source.create_secret("Mail token", "from-bundle").unwrap();
        let bundled = source
            .create_script_instance(draft("notify.sh").secret_input("Token", &token.id))
            .unwrap();
        let archive = source.export_logical_backup().unwrap();
        let tables = archive.staging.path().join("tables");

        // The target links secrets of its own, one of them under a name that
        // differs from the bundle's only by case.
        let mut target = Database::open_in_memory().unwrap();
        target.set_encryption_key(source.encryption_key().unwrap().clone());
        let clash = target.create_secret("MAIL TOKEN", "on-target").unwrap();
        let other = target.create_secret("Other", "on-target").unwrap();
        let local = target
            .create_script_instance(
                draft("local.sh")
                    .secret_input("Token", &clash.id)
                    .secret_input("Other", &other.id),
            )
            .unwrap();
        assert_ne!(clash.id, token.id);

        target
            .import_logical_backup(&tables, &archive.tables, archive.schema_version)
            .unwrap();

        let restored = target.secrets().unwrap();
        assert_eq!(
            restored
                .iter()
                .map(|secret| (
                    secret.id.as_str(),
                    secret.name.as_str(),
                    secret
                        .used_by
                        .iter()
                        .map(|usage| usage.instance_id.as_str())
                        .collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>(),
            [(token.id.as_str(), "Mail token", vec![bundled.id.as_str()])]
        );
        assert_eq!(
            target.script_instances().unwrap(),
            std::slice::from_ref(&bundled)
        );
        assert!(target.script_instance(&local.id).unwrap().is_none());
        assert!(matches!(
            target.script_instance_run_inputs(&bundled.id).unwrap().unwrap()[0].value(),
            OptionValue::Secret(value) if value.expose_for_execution() == "from-bundle"
        ));
    }

    #[test]
    fn bounded_line_reader_rejects_before_buffering_an_oversized_row() {
        let mut reader = BufReader::new(std::io::Cursor::new(vec![b'x'; 9]));
        let mut line = Vec::new();

        let error = read_bounded_line(&mut reader, &mut line, 8).unwrap_err();

        assert!(error.to_string().contains("exceeds 8 bytes"));
        assert!(line.is_empty());
    }

    #[tokio::test]
    async fn older_schema_rows_only_omit_nullable_or_defaulted_columns() {
        let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE rejected (
                id INTEGER PRIMARY KEY,
                optional TEXT,
                defaulted TEXT NOT NULL DEFAULT 'default',
                required TEXT NOT NULL
             );
             CREATE TABLE accepted (
                id INTEGER PRIMARY KEY,
                optional TEXT,
                defaulted TEXT NOT NULL DEFAULT 'default'
             );",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let row_path = root.path().join("rows.ndjson");
        std::fs::write(&row_path, b"{\"id\":1}\n").unwrap();
        let metadata = TablePartMetadata {
            rows: 1,
            columns: vec!["id".into()],
            checksum: String::new(),
        };

        let error = import_sqlite_table(&mut conn, "rejected", &row_path, &metadata, true)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("required target columns: required")
        );

        import_sqlite_table(&mut conn, "accepted", &row_path, &metadata, true)
            .await
            .unwrap();
        let restored: (Option<String>, String) =
            sqlx::query_as("SELECT optional, defaulted FROM accepted WHERE id = 1")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(restored, (None, "default".into()));
    }

    #[tokio::test]
    async fn sqlite_sequence_repair_uses_the_restored_row_floor() {
        let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE sequenced (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL
             )",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO sequenced (id, value) VALUES (100, 'old')")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM sequenced")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO sequenced (id, value) VALUES (7, 'restored')")
            .execute(&mut conn)
            .await
            .unwrap();

        repair_sqlite_sequences(&mut conn).await.unwrap();
        sqlx::query("INSERT INTO sequenced (value) VALUES ('next')")
            .execute(&mut conn)
            .await
            .unwrap();

        let next: i64 = sqlx::query_scalar("SELECT id FROM sequenced WHERE value = 'next'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(next, 8);
    }
}
