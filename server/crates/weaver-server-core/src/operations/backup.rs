use std::path::{Path, PathBuf};

#[cfg(test)]
use std::fs::File;

use sqlx::postgres::{PgPool, PgRow};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{
    Acquire, AssertSqlSafe, Column, ConnectOptions, Postgres, QueryBuilder, Row, Sqlite,
    Transaction, TypeInfo,
};

use crate::StateError;
use crate::persistence::Database;
#[cfg(test)]
use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::sql_runtime::{SqlRuntime, StoreDatastore};
#[cfg(test)]
use crate::rss::RssService;
#[cfg(test)]
use crate::{HistoryFilter, SchedulerHandle};

mod archive;
mod catalog;
mod logical;
mod manifest;
mod pending;
mod permissions;
mod restore;
mod service;

pub use self::logical::TablePartMetadata;
#[cfg(test)]
pub(crate) use self::manifest::{BACKUP_SCOPE, required_category_remaps};
pub use self::manifest::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupServiceError, BackupSourcePaths,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, ManagedPackageInventory,
    RestoreOptions, RestoreReport,
};
pub use self::service::BackupService;
pub use pending::{PendingRestoreOutcome, apply_pending_restore};

const STABLE_TABLES: &[&str] = &[
    "schema_version",
    "settings",
    "servers",
    "server_download_usage",
    "categories",
    "api_keys",
    "job_history",
    "job_history_attributes",
    "job_events",
    "bandwidth_usage_minute_buckets",
    "post_processing_extension_revisions",
    "post_processing_profiles",
    "post_processing_profile_steps",
    "post_processing_profile_assignments",
    "post_processing_job_plans",
    "post_processing_runs",
    "post_processing_attempts",
    "post_processing_log_chunks",
    "rss_feeds",
    "rss_rules",
    "rss_seen_items",
];

const RESTORE_PRISTINE_TABLES: &[&str] = &[
    "metrics_history_chunks",
    "job_history",
    "job_history_attributes",
    "job_events",
    "post_processing_extension_revisions",
    "post_processing_profiles",
    "post_processing_profile_steps",
    "post_processing_profile_assignments",
    "post_processing_job_plans",
    "post_processing_runs",
    "post_processing_attempts",
    "post_processing_log_chunks",
    "active_jobs",
    "active_file_progress",
    "active_files",
    "active_file_identities",
    "active_par2",
    "active_par2_files",
    "active_extracted",
    "active_failed_extractions",
    "active_extraction_chunks",
    "active_archive_headers",
    "active_rar_volume_facts",
    "active_detected_archives",
    "active_volume_status",
    "active_rar_verified_suspect",
];

const CLEAR_IMPORT_TABLES: &[&str] = &[
    "metrics_history_chunks",
    "post_processing_log_chunks",
    "post_processing_attempts",
    "post_processing_runs",
    "post_processing_job_plans",
    "post_processing_profile_assignments",
    "post_processing_profile_steps",
    "post_processing_profiles",
    "post_processing_extension_revisions",
    "rss_seen_items",
    "rss_rules",
    "rss_feeds",
    "integration_events",
    "job_events",
    "job_history_attributes",
    "job_history",
    "api_keys",
    "bandwidth_usage_minute_buckets",
    "categories",
    "server_download_usage",
    "servers",
    "settings",
];

const MIGRATION_LEDGER_TABLE: &str = "_sqlx_migrations";

const SANITIZE_JOB_HISTORY_METADATA_SQL: &str = "UPDATE job_history
        SET metadata = CASE
            WHEN metadata IS NULL OR NOT json_valid(metadata) THEN metadata
            ELSE (
                SELECT COALESCE(json_group_array(json(attr.value)), '[]')
                  FROM json_each(metadata) AS attr
                 WHERE COALESCE(json_extract(attr.value, '$[0]'), '') NOT IN (
                    '__weaver_diagnostic_source_job_id',
                    '__weaver_diagnostic_include_server_hostnames'
                 )
            )
        END";

const REBUILD_JOB_HISTORY_ATTRIBUTES_SQL: &str =
    "INSERT OR IGNORE INTO job_history_attributes (job_id, key, value, completed_at)
     SELECT
         h.job_id,
         json_extract(attr.value, '$[0]') AS key,
         json_extract(attr.value, '$[1]') AS value,
         h.completed_at
     FROM job_history h,
          json_each(CASE WHEN json_valid(h.metadata) THEN h.metadata ELSE '[]' END) AS attr
     WHERE h.metadata IS NOT NULL
       AND json_valid(h.metadata)
       AND json_type(attr.value) = 'array'
       AND json_array_length(attr.value) = 2
       AND json_type(attr.value, '$[0]') = 'text'
       AND json_type(attr.value, '$[1]') = 'text'
       AND json_extract(attr.value, '$[0]') != '__weaver_client_request_id'";

#[derive(Debug, Clone)]
pub struct StableStateExport {
    pub schema_version: i64,
    pub included_tables: Vec<String>,
    pub max_job_id: u64,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

#[cfg(test)]
async fn open_postgres_backup_test_db(test_name: &str) -> Option<(Database, sqlx::PgPool, String)> {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is not set");
        return None;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping {test_name}; WEAVER_TEST_POSTGRES_URL is empty");
        return None;
    }

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!(
        "weaver_backup_{test_name}_{}_{}",
        std::process::id(),
        suffix
    );
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    let create = format!("CREATE SCHEMA {schema}");
    sqlx::query(sqlx::AssertSqlSafe(create))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let mut db = Database::open_target(DatabaseTarget::PostgresUrl(url)).unwrap();
    db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    Some((db, admin, schema))
}

#[cfg(test)]
async fn drop_postgres_backup_test_schema(admin: sqlx::PgPool, schema: String) {
    let drop_schema = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
    sqlx::query(sqlx::AssertSqlSafe(drop_schema))
        .execute(&admin)
        .await
        .unwrap();
}

async fn table_has_column(
    conn: &mut SqliteConnection,
    schema: &'static str,
    table: &'static str,
    column: &'static str,
) -> Result<bool, StateError> {
    let sql = format!("PRAGMA {schema}.table_info({table})");
    let rows = sqlx::query(AssertSqlSafe(sql.as_str()))
        .fetch_all(conn)
        .await
        .map_err(db_err)?;
    for row in rows {
        let name: String = row.try_get("name").map_err(db_err)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn copy_stable_tables_to_backup(
    snapshot_path: PathBuf,
    dest: PathBuf,
) -> Result<(), StateError> {
    let snapshot_path_str = snapshot_path.to_string_lossy().to_string();
    let mut export_conn = SqliteConnectOptions::new()
        .filename(&dest)
        .create_if_missing(true)
        .connect()
        .await
        .map_err(db_err)?;
    sqlx::query("ATTACH DATABASE ? AS src")
        .bind(&snapshot_path_str)
        .execute(&mut export_conn)
        .await
        .map_err(db_err)?;

    create_backup_table_schema(&mut export_conn).await?;

    for table in STABLE_TABLES
        .iter()
        .copied()
        .chain(std::iter::once(MIGRATION_LEDGER_TABLE))
    {
        if table == "job_history_attributes" {
            continue;
        }

        let sql = format!("INSERT INTO {table} SELECT * FROM src.{table}");
        sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
            .execute(&mut export_conn)
            .await
            .map_err(db_err)?;
    }
    rebuild_job_history_attributes(&mut export_conn).await?;
    create_backup_indexes(&mut export_conn).await?;

    sqlx::raw_sql("DETACH DATABASE src")
        .execute(&mut export_conn)
        .await
        .map_err(db_err)?;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut export_conn)
        .await
        .map_err(db_err)?;
    sqlx::Connection::close(export_conn).await.map_err(db_err)?;
    Ok(())
}

async fn copy_postgres_stable_tables_to_backup(
    pool: PgPool,
    dest: PathBuf,
) -> Result<(), StateError> {
    // Use Weaver's SQLite migrations to create the portable interchange schema.
    // The destination may already exist as an empty temporary file.
    let template = Database::open(&dest)?;
    template.close()?;

    let mut conn = SqliteConnectOptions::new()
        .filename(&dest)
        .connect()
        .await
        .map_err(db_err)?;
    let mut tx = conn.begin().await.map_err(db_err)?;

    for table in CLEAR_IMPORT_TABLES {
        let sql = format!("DELETE FROM {table}");
        sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    sqlx::raw_sql("DELETE FROM schema_version")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    for table in STABLE_TABLES {
        let select = format!("SELECT * FROM {table}");
        let rows: Vec<PgRow> = sqlx::query(AssertSqlSafe(select.as_str()))
            .fetch_all(&pool)
            .await
            .map_err(db_err)?;
        for row in rows {
            let columns = row.columns();
            let mut builder = QueryBuilder::<Sqlite>::new(format!("INSERT INTO {table} ("));
            {
                let mut names = builder.separated(", ");
                for column in columns {
                    names.push(column.name());
                }
            }
            builder.push(") VALUES (");
            {
                let mut values = builder.separated(", ");
                for (index, column) in columns.iter().enumerate() {
                    match column.type_info().name() {
                        "BOOL" => {
                            values
                                .push_bind(row.try_get::<Option<bool>, _>(index).map_err(db_err)?);
                        }
                        "INT2" => {
                            values.push_bind(row.try_get::<Option<i16>, _>(index).map_err(db_err)?);
                        }
                        "INT4" => {
                            values.push_bind(row.try_get::<Option<i32>, _>(index).map_err(db_err)?);
                        }
                        "INT8" => {
                            values.push_bind(row.try_get::<Option<i64>, _>(index).map_err(db_err)?);
                        }
                        "FLOAT4" => {
                            values.push_bind(row.try_get::<Option<f32>, _>(index).map_err(db_err)?);
                        }
                        "FLOAT8" => {
                            values.push_bind(row.try_get::<Option<f64>, _>(index).map_err(db_err)?);
                        }
                        "BYTEA" => {
                            values.push_bind(
                                row.try_get::<Option<Vec<u8>>, _>(index).map_err(db_err)?,
                            );
                        }
                        "JSON" | "JSONB" => {
                            let value = row
                                .try_get::<Option<serde_json::Value>, _>(index)
                                .map_err(db_err)?
                                .map(|value| value.to_string());
                            values.push_bind(value);
                        }
                        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" => {
                            values.push_bind(
                                row.try_get::<Option<String>, _>(index).map_err(db_err)?,
                            );
                        }
                        kind => {
                            return Err(StateError::Database(format!(
                                "unsupported PostgreSQL backup column type {kind} for {table}.{}",
                                column.name()
                            )));
                        }
                    }
                }
            }
            builder.push(")");
            builder.build().execute(&mut *tx).await.map_err(db_err)?;
        }
    }

    tx.commit().await.map_err(db_err)?;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut conn)
        .await
        .map_err(db_err)?;
    sqlx::Connection::close(conn).await.map_err(db_err)?;
    Ok(())
}

#[derive(Debug)]
struct PostgresColumn {
    name: String,
    data_type: String,
}

async fn postgres_columns(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
) -> Result<Vec<PostgresColumn>, StateError> {
    let rows = sqlx::query(
        "SELECT column_name, data_type
           FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = $1
          ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(&mut **tx)
    .await
    .map_err(db_err)?;
    rows.into_iter()
        .map(|row| {
            Ok(PostgresColumn {
                name: row.try_get("column_name").map_err(db_err)?,
                data_type: row.try_get("data_type").map_err(db_err)?,
            })
        })
        .collect()
}

fn is_post_processing_table(table: &str) -> bool {
    table.starts_with("post_processing_")
}

async fn import_stable_state_to_postgres(
    pool: PgPool,
    src: PathBuf,
    include_post_processing: bool,
) -> Result<(), StateError> {
    let mut source = SqliteConnectOptions::new()
        .filename(src)
        .connect()
        .await
        .map_err(db_err)?;
    sqlx::raw_sql(SANITIZE_JOB_HISTORY_METADATA_SQL)
        .execute(&mut source)
        .await
        .map_err(db_err)?;
    rebuild_job_history_attributes(&mut source).await?;
    let mut tx = pool.begin().await.map_err(db_err)?;

    sqlx::raw_sql(
        "UPDATE job_history SET post_processing_run_id = NULL;
         UPDATE post_processing_runs SET rerun_of_run_id = NULL;",
    )
    .execute(&mut *tx)
    .await
    .map_err(db_err)?;
    for table in CLEAR_IMPORT_TABLES {
        let sql = format!("DELETE FROM {table}");
        sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }

    let mut history_run_links = Vec::<(i64, String)>::new();
    let mut rerun_links = Vec::<(String, String)>::new();
    for table in STABLE_TABLES {
        if *table == "schema_version"
            || (!include_post_processing && is_post_processing_table(table))
        {
            continue;
        }

        let columns = postgres_columns(&mut tx, table).await?;
        if columns.is_empty() {
            return Err(StateError::Database(format!(
                "restore target is missing table {table}"
            )));
        }
        let column_names = columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let select = format!("SELECT {column_names} FROM {table}");
        let rows: Vec<SqliteRow> = sqlx::query(AssertSqlSafe(select.as_str()))
            .fetch_all(&mut source)
            .await
            .map_err(db_err)?;

        for row in rows {
            let mut builder = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO {table} ({column_names}) VALUES ("
            ));
            {
                let mut values = builder.separated(", ");
                for column in &columns {
                    if *table == "job_history" && column.name == "post_processing_run_id" {
                        if include_post_processing {
                            let job_id: i64 = row.try_get("job_id").map_err(db_err)?;
                            if let Some(run_id) = row
                                .try_get::<Option<String>, _>(column.name.as_str())
                                .map_err(db_err)?
                            {
                                history_run_links.push((job_id, run_id));
                            }
                        }
                        values.push_bind(Option::<String>::None);
                        continue;
                    }
                    if *table == "post_processing_runs" && column.name == "rerun_of_run_id" {
                        let run_id: String = row.try_get("run_id").map_err(db_err)?;
                        if let Some(rerun_of) = row
                            .try_get::<Option<String>, _>(column.name.as_str())
                            .map_err(db_err)?
                        {
                            rerun_links.push((run_id, rerun_of));
                        }
                        values.push_bind(Option::<String>::None);
                        continue;
                    }

                    match column.data_type.as_str() {
                        "boolean" => {
                            values.push_bind(
                                row.try_get::<Option<bool>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        "smallint" => {
                            let value = row
                                .try_get::<Option<i64>, _>(column.name.as_str())
                                .map_err(db_err)?
                                .map(i16::try_from)
                                .transpose()
                                .map_err(db_err)?;
                            values.push_bind(value);
                        }
                        "integer" => {
                            let value = row
                                .try_get::<Option<i64>, _>(column.name.as_str())
                                .map_err(db_err)?
                                .map(i32::try_from)
                                .transpose()
                                .map_err(db_err)?;
                            values.push_bind(value);
                        }
                        "bigint" => {
                            values.push_bind(
                                row.try_get::<Option<i64>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        "real" => {
                            values.push_bind(
                                row.try_get::<Option<f32>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        "double precision" => {
                            values.push_bind(
                                row.try_get::<Option<f64>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        "bytea" => {
                            values.push_bind(
                                row.try_get::<Option<Vec<u8>>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        "json" | "jsonb" => {
                            let value = row
                                .try_get::<Option<String>, _>(column.name.as_str())
                                .map_err(db_err)?
                                .map(|raw| serde_json::from_str::<serde_json::Value>(&raw))
                                .transpose()
                                .map_err(db_err)?;
                            values.push_bind(value);
                        }
                        "text" | "character varying" | "character" => {
                            values.push_bind(
                                row.try_get::<Option<String>, _>(column.name.as_str())
                                    .map_err(db_err)?,
                            );
                        }
                        kind => {
                            return Err(StateError::Database(format!(
                                "unsupported PostgreSQL restore column type {kind} for {table}.{}",
                                column.name
                            )));
                        }
                    }
                }
            }
            builder.push(")");
            builder.build().execute(&mut *tx).await.map_err(db_err)?;
        }
    }

    for (run_id, rerun_of) in rerun_links {
        sqlx::query(
            "UPDATE post_processing_runs
                SET rerun_of_run_id = $1
              WHERE run_id = $2",
        )
        .bind(rerun_of)
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    }
    for (job_id, run_id) in history_run_links {
        sqlx::query(
            "UPDATE job_history
                SET post_processing_run_id = $1
              WHERE job_id = $2",
        )
        .bind(run_id)
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    }

    for (table, column) in [
        ("api_keys", "id"),
        ("job_events", "id"),
        ("rss_feeds", "id"),
        ("rss_rules", "id"),
    ] {
        let sql = format!(
            "SELECT setval(pg_get_serial_sequence('{table}', '{column}'),
                           COALESCE(MAX({column}), 1), COUNT(*) > 0)
               FROM {table}"
        );
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }

    tx.commit().await.map_err(db_err)?;
    Ok(())
}

async fn create_backup_table_schema(conn: &mut SqliteConnection) -> Result<(), StateError> {
    for table in STABLE_TABLES
        .iter()
        .copied()
        .chain(std::iter::once(MIGRATION_LEDGER_TABLE))
    {
        let create_sql: String = sqlx::query_scalar(
            "SELECT sql
               FROM src.sqlite_master
              WHERE type = 'table'
                AND name = ?",
        )
        .bind(table)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?
        .ok_or_else(|| StateError::Database(format!("source database is missing {table}")))?;
        sqlx::raw_sql(AssertSqlSafe(create_sql.as_str()))
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }
    Ok(())
}

async fn create_backup_indexes(conn: &mut SqliteConnection) -> Result<(), StateError> {
    for table in STABLE_TABLES
        .iter()
        .copied()
        .chain(std::iter::once(MIGRATION_LEDGER_TABLE))
    {
        let rows = sqlx::query(
            "SELECT sql
               FROM src.sqlite_master
              WHERE type = 'index'
                AND tbl_name = ?
                AND sql IS NOT NULL
              ORDER BY name",
        )
        .bind(table)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
        for row in rows {
            let create_sql: String = row.try_get("sql").map_err(db_err)?;
            sqlx::raw_sql(AssertSqlSafe(create_sql.as_str()))
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
        }
    }
    Ok(())
}

async fn rebuild_job_history_attributes(conn: &mut SqliteConnection) -> Result<(), StateError> {
    sqlx::raw_sql("DELETE FROM job_history_attributes")
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    sqlx::raw_sql(REBUILD_JOB_HISTORY_ATTRIBUTES_SQL)
        .execute(conn)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn rebuild_job_history_attributes_tx(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<(), StateError> {
    sqlx::raw_sql("DELETE FROM job_history_attributes")
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::raw_sql(REBUILD_JOB_HISTORY_ATTRIBUTES_SQL)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    Ok(())
}

impl Database {
    pub fn schema_version(&self) -> Result<i64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT version FROM schema_version LIMIT 1",
                &[],
            )
            .await?
            .ok_or_else(|| StateError::Database("schema_version table is empty".to_string()))?
            .i64("version")
        })
    }

    pub fn restore_target_is_pristine(&self) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            for table in RESTORE_PRISTINE_TABLES {
                let query = format!("SELECT COUNT(*) AS count FROM {table}");
                let count = SqlRuntime::fetch_optional(datastore.read_exec(), &query, &[])
                    .await?
                    .map(|row| row.i64("count"))
                    .transpose()?
                    .unwrap_or(0);
                if count > 0 {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    pub fn export_stable_state(&self, dest: &Path) -> Result<StableStateExport, StateError> {
        let datastore = self.datastore();
        let schema_version = self.schema_version()?;
        let max_job_id = self.max_job_id_all()?;
        match datastore {
            datastore @ StoreDatastore::Sqlite { .. } => {
                let snapshot_dir = tempfile::tempdir().map_err(db_err)?;
                let snapshot_path = snapshot_dir.path().join("snapshot.db");
                let snapshot_path_str = snapshot_path.to_string_lossy().to_string();

                self.run_sql_blocking_local({
                    let snapshot_path_str = snapshot_path_str.clone();
                    move || async move {
                        SqlRuntime::run_serialized_sqlite_connection(
                            &datastore,
                            "export_stable_state_vacuum_into",
                            move |mut conn| {
                                let snapshot_path_str = snapshot_path_str.clone();
                                async move {
                                    sqlx::query("VACUUM INTO ?")
                                        .bind(&snapshot_path_str)
                                        .execute(&mut *conn)
                                        .await
                                        .map_err(db_err)?;
                                    Ok(())
                                }
                            },
                        )
                        .await
                    }
                })?;

                self.run_sql_blocking_local({
                    let snapshot_path = snapshot_path.clone();
                    let dest = dest.to_path_buf();
                    move || async move { copy_stable_tables_to_backup(snapshot_path, dest).await }
                })?;
            }
            StoreDatastore::Postgres { pool } => {
                let dest = dest.to_path_buf();
                self.run_sql_blocking(copy_postgres_stable_tables_to_backup(pool, dest))?;
            }
        }

        Ok(StableStateExport {
            schema_version,
            included_tables: STABLE_TABLES.iter().map(|t| (*t).to_string()).collect(),
            max_job_id,
        })
    }

    pub fn import_stable_state(&self, src: &Path) -> Result<(), StateError> {
        self.import_stable_state_with_post_processing(src, true)
    }

    pub(crate) fn import_stable_state_with_post_processing(
        &self,
        src: &Path,
        include_post_processing: bool,
    ) -> Result<(), StateError> {
        let src_path = src.to_string_lossy().to_string();
        let datastore = self.datastore();
        if let StoreDatastore::Postgres { pool } = datastore {
            let src = src.to_path_buf();
            return self.run_sql_blocking(import_stable_state_to_postgres(
                pool,
                src,
                include_post_processing,
            ));
        }
        let datastore = self.datastore();
        self.run_sql_blocking_local(move || async move {
            SqlRuntime::run_serialized_sqlite_connection(
                &datastore,
                "import_stable_state",
                move |mut conn| {
                    let src_path = src_path.clone();
                    async move {
                        sqlx::query("ATTACH DATABASE ? AS src")
                            .bind(&src_path)
                            .execute(&mut *conn)
                            .await
                            .map_err(db_err)?;

                        let import_result = async {
                            let src_optional_recovery_bytes = if table_has_column(
                                &mut conn,
                                "src",
                                "job_history",
                                "optional_recovery_bytes",
                            )
                            .await?
                            {
                                "optional_recovery_bytes"
                            } else {
                                "0"
                            };
                            let src_optional_recovery_downloaded_bytes = if table_has_column(
                                &mut conn,
                                "src",
                                "job_history",
                                "optional_recovery_downloaded_bytes",
                            )
                            .await?
                            {
                                "optional_recovery_downloaded_bytes"
                            } else {
                                "0"
                            };
                            let src_job_hash = if table_has_column(
                                &mut conn,
                                "src",
                                "job_history",
                                "job_hash",
                            )
                            .await?
                            {
                                "job_hash"
                            } else {
                                "NULL"
                            };
                            let src_pipeline_outcome = if table_has_column(
                                &mut conn,
                                "src",
                                "job_history",
                                "pipeline_outcome_json",
                            )
                            .await?
                            {
                                "pipeline_outcome_json"
                            } else {
                                "NULL"
                            };
                            let src_post_processing_summary = if table_has_column(
                                &mut conn,
                                "src",
                                "job_history",
                                "post_processing_summary",
                            )
                            .await?
                            {
                                "post_processing_summary"
                            } else {
                                "'not_run'"
                            };
                            let src_post_processing_run_id = if include_post_processing
                                && table_has_column(
                                    &mut conn,
                                    "src",
                                    "job_history",
                                    "post_processing_run_id",
                                )
                                .await?
                            {
                                "post_processing_run_id"
                            } else {
                                "NULL"
                            };
                            let src_server_backfill =
                                if table_has_column(&mut conn, "src", "servers", "backfill")
                                    .await?
                                {
                                    "backfill"
                                } else {
                                    "0"
                                };
                            let src_server_retention_days = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "retention_days",
                            )
                            .await?
                            {
                                "retention_days"
                            } else {
                                "0"
                            };
                            let src_server_max_download_speed = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "max_download_speed",
                            )
                            .await?
                            {
                                "max_download_speed"
                            } else {
                                "0"
                            };
                            let src_server_quota_enabled = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_enabled",
                            )
                            .await?
                            {
                                "download_quota_enabled"
                            } else {
                                "0"
                            };
                            let src_server_quota_limit = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_limit_bytes",
                            )
                            .await?
                            {
                                "download_quota_limit_bytes"
                            } else {
                                "0"
                            };
                            let src_server_quota_period = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_period",
                            )
                            .await?
                            {
                                "download_quota_period"
                            } else {
                                "'one_time'"
                            };
                            let src_server_quota_reset_time = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_reset_time_minutes_local",
                            )
                            .await?
                            {
                                "download_quota_reset_time_minutes_local"
                            } else {
                                "0"
                            };
                            let src_server_quota_weekday = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_weekly_reset_weekday",
                            )
                            .await?
                            {
                                "download_quota_weekly_reset_weekday"
                            } else {
                                "'mon'"
                            };
                            let src_server_quota_month_day = if table_has_column(
                                &mut conn,
                                "src",
                                "servers",
                                "download_quota_monthly_reset_day",
                            )
                            .await?
                            {
                                "download_quota_monthly_reset_day"
                            } else {
                                "1"
                            };
                            let src_server_tls_ca_cert =
                                if table_has_column(&mut conn, "src", "servers", "tls_ca_cert")
                                    .await?
                                {
                                    "tls_ca_cert"
                                } else {
                                    "NULL"
                                };
                            let src_has_server_download_usage = table_has_column(
                                &mut conn,
                                "src",
                                "server_download_usage",
                                "server_id",
                            )
                            .await?;
                            let import_server_download_usage = if src_has_server_download_usage {
                                "INSERT INTO server_download_usage
                                     (server_id, lifetime_bytes, quota_baseline_bytes,
                                      window_start_epoch_seconds, window_end_epoch_seconds,
                                      updated_at_epoch_seconds)
                                     SELECT server_id, lifetime_bytes, quota_baseline_bytes,
                                            window_start_epoch_seconds, window_end_epoch_seconds,
                                            updated_at_epoch_seconds
                                       FROM src.server_download_usage;"
                            } else {
                                ""
                            };
                            let import_post_processing = if include_post_processing {
                                "INSERT INTO post_processing_extension_revisions
                                     (extension_id, revision_id, declared_version, digest, adapter,
                                      display_name, compatibility_name, entrypoint, manifest_json,
                                      managed_path, discovered_source_path, trust_state,
                                      discovered_at_epoch_ms, approved_at_epoch_ms)
                                     SELECT extension_id, revision_id, declared_version, digest, adapter,
                                            display_name, compatibility_name, entrypoint, manifest_json,
                                            managed_path, discovered_source_path, trust_state,
                                            discovered_at_epoch_ms, approved_at_epoch_ms
                                       FROM src.post_processing_extension_revisions;
                                 INSERT INTO post_processing_profiles
                                     (profile_id, name, enabled, created_at_epoch_ms, updated_at_epoch_ms)
                                     SELECT profile_id, name, enabled, created_at_epoch_ms, updated_at_epoch_ms
                                       FROM src.post_processing_profiles;
                                 INSERT INTO post_processing_profile_steps
                                     (profile_id, step_index, selection_json, policy_json,
                                      options_json, secret_options_json)
                                     SELECT profile_id, step_index, selection_json, policy_json,
                                            options_json, secret_options_json
                                       FROM src.post_processing_profile_steps;
                                 INSERT INTO post_processing_profile_assignments
                                     (scope_kind, scope_key, profile_id)
                                     SELECT scope_kind, scope_key, profile_id
                                       FROM src.post_processing_profile_assignments;
                                 INSERT INTO post_processing_job_plans
                                     (job_id, provenance_json, plan_json, secret_options_json,
                                      created_at_epoch_ms)
                                     SELECT job_id, provenance_json, plan_json, secret_options_json,
                                            created_at_epoch_ms
                                       FROM src.post_processing_job_plans;
                                 INSERT INTO post_processing_runs
                                     (run_id, job_id, status, pipeline_outcome_json, summary,
                                      terminal_intent, plan_json, secret_options_json,
                                      rerun_of_run_id, queued_at_epoch_ms, queue_position,
                                      started_at_epoch_ms, finished_at_epoch_ms)
                                     SELECT run_id, job_id, status, pipeline_outcome_json, summary,
                                            terminal_intent, plan_json, secret_options_json,
                                            rerun_of_run_id, queued_at_epoch_ms, queue_position,
                                            started_at_epoch_ms, finished_at_epoch_ms
                                       FROM src.post_processing_runs;
                                 INSERT INTO post_processing_attempts
                                     (attempt_id, run_id, step_index, status, extension_id,
                                      revision_id, adapter, command_json, working_directory,
                                      exit_code, error_message, progress_json, control_token_hash,
                                      output_truncated, queued_at_epoch_ms, started_at_epoch_ms,
                                      finished_at_epoch_ms)
                                     SELECT attempt_id, run_id, step_index, status, extension_id,
                                            revision_id, adapter, command_json, working_directory,
                                            exit_code, error_message, progress_json, control_token_hash,
                                            output_truncated, queued_at_epoch_ms, started_at_epoch_ms,
                                            finished_at_epoch_ms
                                       FROM src.post_processing_attempts;
                                 INSERT INTO post_processing_log_chunks
                                     (attempt_id, sequence, stream, payload, byte_count,
                                      created_at_epoch_ms)
                                     SELECT attempt_id, sequence, stream, payload, byte_count,
                                            created_at_epoch_ms
                                       FROM src.post_processing_log_chunks;"
                            } else {
                                ""
                            };

                            let mut tx = conn.begin().await.map_err(db_err)?;
                            for table in CLEAR_IMPORT_TABLES {
                                let sql = format!("DELETE FROM {table}");
                                sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
                                    .execute(&mut *tx)
                                    .await
                                    .map_err(db_err)?;
                            }

                            let import_sql = format!(
                                "INSERT INTO settings (key, value)
                                     SELECT key, value FROM src.settings;
                                 INSERT INTO servers
                                     (id, host, port, tls, username, password, connections, active,
                                      supports_pipelining, priority, backfill, retention_days,
                                      max_download_speed, download_quota_enabled,
                                      download_quota_limit_bytes, download_quota_period,
                                      download_quota_reset_time_minutes_local,
                                      download_quota_weekly_reset_weekday,
                                      download_quota_monthly_reset_day, tls_ca_cert)
                                     SELECT id, host, port, tls, username, password, connections, active,
                                            supports_pipelining, priority, {src_server_backfill},
                                            {src_server_retention_days}, {src_server_max_download_speed},
                                            {src_server_quota_enabled}, {src_server_quota_limit},
                                            {src_server_quota_period}, {src_server_quota_reset_time},
                                            {src_server_quota_weekday}, {src_server_quota_month_day},
                                            {src_server_tls_ca_cert}
                                       FROM src.servers;
                                 {import_server_download_usage}
                                 INSERT INTO categories (id, name, dest_dir, aliases)
                                     SELECT id, name, dest_dir, aliases FROM src.categories;
                                 INSERT INTO api_keys (id, name, key_hash, scope, created_at, last_used_at)
                                     SELECT id, name, key_hash, scope, created_at, last_used_at FROM src.api_keys;
                                 INSERT INTO job_history
                                     (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                                      optional_recovery_bytes, optional_recovery_downloaded_bytes,
                                      failed_bytes, health, category, output_dir, nzb_path, created_at,
                                      completed_at, metadata, pipeline_outcome_json,
                                      post_processing_summary, post_processing_run_id)
                                     SELECT job_id, {src_job_hash}, name, status, error_message, total_bytes, downloaded_bytes,
                                            {src_optional_recovery_bytes}, {src_optional_recovery_downloaded_bytes},
                                            failed_bytes, health, category, output_dir, nzb_path, created_at, completed_at,
                                            CASE
                                                WHEN metadata IS NULL OR NOT json_valid(metadata) THEN metadata
                                                ELSE (
                                                    SELECT COALESCE(json_group_array(json(attr.value)), '[]')
                                                      FROM json_each(metadata) AS attr
                                                     WHERE COALESCE(json_extract(attr.value, '$[0]'), '') NOT IN (
                                                        '__weaver_diagnostic_source_job_id',
                                                        '__weaver_diagnostic_include_server_hostnames'
                                                     )
                                                )
                                            END AS metadata,
                                            {src_pipeline_outcome}, {src_post_processing_summary},
                                            {src_post_processing_run_id}
                                     FROM src.job_history;
                                 INSERT INTO job_events (id, job_id, timestamp, kind, message, file_id)
                                     SELECT id, job_id, timestamp, kind, message, file_id FROM src.job_events;
                                 {import_post_processing}
                                 INSERT INTO bandwidth_usage_minute_buckets (bucket_epoch_minute, payload_bytes)
                                     SELECT bucket_epoch_minute, payload_bytes FROM src.bandwidth_usage_minute_buckets;
                                 INSERT INTO rss_feeds
                                     (id, name, url, enabled, poll_interval_secs, username, password, default_category, default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error, consecutive_failures)
                                     SELECT id, name, url, enabled, poll_interval_secs, username, password, default_category, default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error, consecutive_failures
                                     FROM src.rss_feeds;
                                 INSERT INTO rss_rules
                                     (id, feed_id, sort_order, enabled, action, title_regex, item_categories, min_size_bytes, max_size_bytes, category_override, metadata)
                                     SELECT id, feed_id, sort_order, enabled, action, title_regex, item_categories, min_size_bytes, max_size_bytes, category_override, metadata
                                     FROM src.rss_rules;
                                 INSERT INTO rss_seen_items
                                     (feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at, job_id, item_url, error)
                                     SELECT feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at, job_id, item_url, error
                                     FROM src.rss_seen_items;
                                 DELETE FROM sqlite_sequence WHERE name IN ('api_keys', 'job_events');
                                 INSERT INTO sqlite_sequence (name, seq)
                                     SELECT 'api_keys', COALESCE(MAX(id), 0) FROM api_keys;
                                 INSERT INTO sqlite_sequence (name, seq)
                                     SELECT 'job_events', COALESCE(MAX(id), 0) FROM job_events;
                                 ",
                            );
                            sqlx::raw_sql(AssertSqlSafe(import_sql.as_str()))
                            .execute(&mut *tx)
                            .await
                            .map_err(db_err)?;
                            rebuild_job_history_attributes_tx(&mut tx).await?;

                            tx.commit().await.map_err(db_err)?;
                            Ok::<(), StateError>(())
                        }
                        .await;

                        let detach_result = sqlx::raw_sql("DETACH DATABASE src")
                            .execute(&mut *conn)
                            .await
                            .map_err(db_err);
                        import_result?;
                        detach_result?;
                        Ok(())
                    }
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod backup_service_tests;
