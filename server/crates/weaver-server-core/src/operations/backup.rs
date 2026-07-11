use std::path::{Path, PathBuf};

#[cfg(test)]
use std::fs::File;

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{Acquire, AssertSqlSafe, ConnectOptions, Row, Sqlite, Transaction};

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlEngine, SqlRuntime};
#[cfg(test)]
use crate::rss::RssService;
#[cfg(test)]
use crate::{HistoryFilter, SchedulerHandle};

mod archive;
mod manifest;
mod restore;
mod service;

#[cfg(test)]
pub(crate) use self::manifest::{BACKUP_SCOPE, required_category_remaps};
pub use self::manifest::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupServiceError, BackupSourcePaths,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
};
pub use self::service::BackupService;

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
    "rss_feeds",
    "rss_rules",
    "rss_seen_items",
];

const RESTORE_PRISTINE_TABLES: &[&str] = &[
    "metrics_history_chunks",
    "job_history",
    "job_history_attributes",
    "job_events",
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
        if datastore.engine() != SqlEngine::Sqlite {
            return Err(StateError::Database(
                "stable-state backup export currently requires sqlite datastore".to_string(),
            ));
        }

        let snapshot_dir = tempfile::tempdir().map_err(db_err)?;
        let snapshot_path = snapshot_dir.path().join("snapshot.db");
        let snapshot_path_str = snapshot_path.to_string_lossy().to_string();

        let schema_version = self.schema_version()?;
        let max_job_id = self.max_job_id_all()?;

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

        Ok(StableStateExport {
            schema_version,
            included_tables: STABLE_TABLES.iter().map(|t| (*t).to_string()).collect(),
            max_job_id,
        })
    }

    pub fn import_stable_state(&self, src: &Path) -> Result<(), StateError> {
        let src_path = src.to_string_lossy().to_string();
        let datastore = self.datastore();
        if datastore.engine() != SqlEngine::Sqlite {
            return Err(StateError::Database(
                "stable-state backup import currently requires sqlite datastore".to_string(),
            ));
        }
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
                                     (job_id, name, status, error_message, total_bytes, downloaded_bytes,
                                      optional_recovery_bytes, optional_recovery_downloaded_bytes,
                                      failed_bytes, health, category, output_dir, nzb_path, created_at, completed_at, metadata)
                                     SELECT job_id, name, status, error_message, total_bytes, downloaded_bytes,
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
                                            END AS metadata
                                     FROM src.job_history;
                                 INSERT INTO job_events (id, job_id, timestamp, kind, message, file_id)
                                     SELECT id, job_id, timestamp, kind, message, file_id FROM src.job_events;
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
