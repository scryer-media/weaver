use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
use std::fs::File;

use rusqlite::Connection;

use crate::StateError;
use crate::persistence::Database;
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
    "categories",
    "api_keys",
    "job_history",
    "job_events",
    "integration_events",
    "bandwidth_usage_minute_buckets",
    "rss_feeds",
    "rss_rules",
    "rss_seen_items",
];

const RESTORE_PRISTINE_TABLES: &[&str] = &[
    "metrics_scrapes",
    "job_history",
    "job_events",
    "active_jobs",
    "active_segments",
    "active_file_progress",
    "active_files",
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
    "metrics_scrapes",
    "rss_seen_items",
    "rss_rules",
    "rss_feeds",
    "integration_events",
    "job_events",
    "job_history",
    "api_keys",
    "bandwidth_usage_minute_buckets",
    "categories",
    "servers",
    "settings",
];

#[derive(Debug, Clone)]
pub struct StableStateExport {
    pub schema_version: i64,
    pub included_tables: Vec<String>,
    pub max_job_id: u64,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

fn table_has_column(
    conn: &Connection,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<bool, StateError> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA {schema}.table_info({table})"))
        .map_err(db_err)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(db_err)?;
    for row in rows {
        if row.map_err(db_err)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Database {
    pub fn schema_version(&self) -> Result<i64, StateError> {
        let conn = self.conn();
        conn.query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
            row.get(0)
        })
        .map_err(db_err)
    }

    pub fn restore_target_is_pristine(&self) -> Result<bool, StateError> {
        let conn = self.conn();
        for table in RESTORE_PRISTINE_TABLES {
            let query = format!("SELECT COUNT(*) FROM {table}");
            let count: i64 = conn
                .query_row(&query, [], |row| row.get(0))
                .map_err(db_err)?;
            if count > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn export_stable_state(&self, dest: &Path) -> Result<StableStateExport, StateError> {
        let snapshot_dir = tempfile::tempdir().map_err(db_err)?;
        let snapshot_path = snapshot_dir.path().join("snapshot.db");
        let snapshot_path_str = snapshot_path.to_string_lossy().to_string();

        let schema_version = self.schema_version()?;
        let max_job_id = self.max_job_id_all()?;

        {
            let conn = self.conn();
            conn.execute("VACUUM INTO ?1", [&snapshot_path_str])
                .map_err(db_err)?;
        }

        let export_conn = Connection::open(dest).map_err(db_err)?;
        export_conn
            .execute("ATTACH DATABASE ?1 AS src", [&snapshot_path_str])
            .map_err(db_err)?;

        for table in STABLE_TABLES {
            let sql = format!("CREATE TABLE {table} AS SELECT * FROM src.{table}");
            export_conn.execute_batch(&sql).map_err(db_err)?;
        }

        export_conn
            .execute_batch("DETACH DATABASE src")
            .map_err(db_err)?;

        Ok(StableStateExport {
            schema_version,
            included_tables: STABLE_TABLES.iter().map(|t| (*t).to_string()).collect(),
            max_job_id,
        })
    }

    pub fn import_stable_state(&self, src: &Path) -> Result<(), StateError> {
        let src_path = src.to_string_lossy().to_string();
        let conn = self.conn();
        conn.execute("ATTACH DATABASE ?1 AS src", [&src_path])
            .map_err(db_err)?;
        let src_optional_recovery_bytes =
            if table_has_column(&conn, "src", "job_history", "optional_recovery_bytes")? {
                "optional_recovery_bytes"
            } else {
                "0"
            };
        let src_optional_recovery_downloaded_bytes = if table_has_column(
            &conn,
            "src",
            "job_history",
            "optional_recovery_downloaded_bytes",
        )? {
            "optional_recovery_downloaded_bytes"
        } else {
            "0"
        };
        let tx = conn.unchecked_transaction().map_err(db_err)?;

        for table in CLEAR_IMPORT_TABLES {
            let sql = format!("DELETE FROM {table}");
            tx.execute(&sql, []).map_err(db_err)?;
        }

        tx.execute_batch(&format!(
            "INSERT INTO settings (key, value)
                 SELECT key, value FROM src.settings;
             INSERT INTO servers (id, host, port, tls, username, password, connections, active, supports_pipelining, priority)
                 SELECT id, host, port, tls, username, password, connections, active, supports_pipelining, priority FROM src.servers;
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
                        failed_bytes, health, category, output_dir, nzb_path, created_at, completed_at, metadata
                 FROM src.job_history;
             INSERT INTO job_events (id, job_id, timestamp, kind, message, file_id)
                 SELECT id, job_id, timestamp, kind, message, file_id FROM src.job_events;
             INSERT INTO integration_events (id, timestamp, kind, item_id, payload_json)
                 SELECT id, timestamp, kind, item_id, payload_json FROM src.integration_events;
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
             DELETE FROM sqlite_sequence WHERE name IN ('api_keys', 'job_events', 'integration_events');
             INSERT INTO sqlite_sequence (name, seq)
                 SELECT 'api_keys', COALESCE(MAX(id), 0) FROM api_keys;
             INSERT INTO sqlite_sequence (name, seq)
                 SELECT 'job_events', COALESCE(MAX(id), 0) FROM job_events;
             INSERT INTO sqlite_sequence (name, seq)
                 SELECT 'integration_events', COALESCE(MAX(id), 0) FROM integration_events;
             ",
        ))
        .map_err(db_err)?;

        tx.commit().map_err(db_err)?;
        conn.execute_batch("DETACH DATABASE src").map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod backup_service_tests;
