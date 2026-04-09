use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::StateError;

pub use crate::auth::{ApiKeyRow, AuthCredentials};
pub use crate::history::{HistoryFilter, IntegrationEventRow, JobEvent, JobHistoryRow};
pub use crate::jobs::{
    ActiveFileProgress, ActiveJob, ActivePar2File, CommittedSegment, ExtractionChunk, RecoveredJob,
};
pub use crate::operations::{MetricsScrapeRow, StableStateExport};
pub use crate::rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};

const SCHEMA_VERSION: i64 = 17;

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), StateError> {
    let exists: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1"),
            [column],
            |row| row.get(0),
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
    if exists == 0 {
        conn.execute_batch(&format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition};"
        ))
        .map_err(|e| StateError::Database(e.to_string()))?;
    }
    Ok(())
}

/// SQLite-backed persistent store for config, servers, and job history.
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
    encryption_key: Option<crate::persistence::encryption::EncryptionKey>,
}

impl Database {
    /// Open (or create) the database at `path`.
    /// Runs schema creation and sets WAL mode.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let conn = Connection::open(path).map_err(|e| StateError::Database(e.to_string()))?;

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;
             PRAGMA auto_vacuum=INCREMENTAL;
             PRAGMA cache_size=-32000;
             PRAGMA mmap_size=67108864;
             PRAGMA temp_store=MEMORY;",
        )
        .map_err(|e| StateError::Database(e.to_string()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            encryption_key: None,
        };
        db.create_schema()?;
        Ok(db)
    }

    /// Open an in-memory database (for tests).
    pub fn open_in_memory() -> Result<Self, StateError> {
        let conn = Connection::open_in_memory().map_err(|e| StateError::Database(e.to_string()))?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            encryption_key: None,
        };
        db.create_schema()?;
        Ok(db)
    }

    /// Set the encryption key used to protect sensitive fields (passwords).
    pub fn set_encryption_key(&mut self, key: crate::persistence::encryption::EncryptionKey) {
        self.encryption_key = Some(key);
    }

    /// Get a reference to the encryption key, if set.
    pub(crate) fn encryption_key(&self) -> Option<&crate::persistence::encryption::EncryptionKey> {
        self.encryption_key.as_ref()
    }

    /// Check if the database has no settings (i.e. fresh / needs migration).
    pub fn is_empty(&self) -> Result<bool, StateError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM settings", [], |row| row.get(0))
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(count == 0)
    }

    fn create_schema(&self) -> Result<(), StateError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS servers (
                id                  INTEGER PRIMARY KEY NOT NULL,
                host                TEXT NOT NULL,
                port                INTEGER NOT NULL,
                tls                 INTEGER NOT NULL DEFAULT 1,
                username            TEXT,
                password            TEXT,
                connections         INTEGER NOT NULL DEFAULT 10,
                active              INTEGER NOT NULL DEFAULT 1,
                supports_pipelining INTEGER NOT NULL DEFAULT 0,
                priority            INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS job_history (
                job_id           INTEGER PRIMARY KEY NOT NULL,
                name             TEXT NOT NULL,
                status           TEXT NOT NULL,
                error_message    TEXT,
                total_bytes      INTEGER NOT NULL DEFAULT 0,
                downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                optional_recovery_bytes INTEGER NOT NULL DEFAULT 0,
                optional_recovery_downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                failed_bytes     INTEGER NOT NULL DEFAULT 0,
                health           INTEGER NOT NULL DEFAULT 1000,
                category         TEXT,
                output_dir       TEXT,
                nzb_path         TEXT,
                created_at       INTEGER NOT NULL,
                completed_at     INTEGER NOT NULL,
                metadata         TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_job_history_completed_at
                ON job_history(completed_at);

            CREATE TABLE IF NOT EXISTS active_jobs (
                job_id       INTEGER PRIMARY KEY NOT NULL,
                nzb_hash     BLOB NOT NULL,
                nzb_path     TEXT NOT NULL,
                output_dir   TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'downloading',
                error        TEXT,
                created_at   INTEGER NOT NULL,
                normalization_retried INTEGER NOT NULL DEFAULT 0,
                queued_repair_at_epoch_ms REAL,
                queued_extract_at_epoch_ms REAL,
                paused_resume_status TEXT,
                category     TEXT,
                metadata     TEXT
            );

            CREATE TABLE IF NOT EXISTS active_segments (
                job_id          INTEGER NOT NULL,
                file_index      INTEGER NOT NULL,
                segment_number  INTEGER NOT NULL,
                file_offset     INTEGER NOT NULL,
                decoded_size    INTEGER NOT NULL,
                crc32           INTEGER NOT NULL,
                PRIMARY KEY (job_id, file_index, segment_number)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_file_progress (
                job_id                  INTEGER NOT NULL,
                file_index              INTEGER NOT NULL,
                contiguous_bytes_written INTEGER NOT NULL,
                PRIMARY KEY (job_id, file_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_files (
                job_id      INTEGER NOT NULL,
                file_index  INTEGER NOT NULL,
                filename    TEXT NOT NULL,
                md5         BLOB NOT NULL,
                PRIMARY KEY (job_id, file_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_par2 (
                job_id               INTEGER PRIMARY KEY NOT NULL,
                slice_size           INTEGER NOT NULL,
                recovery_block_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS active_par2_files (
                job_id               INTEGER NOT NULL,
                file_index           INTEGER NOT NULL,
                filename             TEXT NOT NULL,
                recovery_block_count INTEGER NOT NULL DEFAULT 0,
                promoted             INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (job_id, file_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_extracted (
                job_id      INTEGER NOT NULL,
                member_name TEXT NOT NULL,
                output_path TEXT NOT NULL,
                PRIMARY KEY (job_id, member_name)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_failed_extractions (
                job_id      INTEGER NOT NULL,
                member_name TEXT NOT NULL,
                PRIMARY KEY (job_id, member_name)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS job_events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id    INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                kind      TEXT NOT NULL,
                message   TEXT NOT NULL,
                file_id   TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_job_events_job_id
                ON job_events(job_id);

            CREATE TABLE IF NOT EXISTS integration_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    INTEGER NOT NULL,
                kind         TEXT NOT NULL,
                item_id      INTEGER,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_integration_events_item_id
                ON integration_events(item_id);

            CREATE TABLE IF NOT EXISTS metrics_scrapes (
                scraped_at_epoch_sec INTEGER PRIMARY KEY NOT NULL,
                body_zstd            BLOB NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_extraction_chunks (
                job_id        INTEGER NOT NULL,
                set_name      TEXT NOT NULL,
                member_name   TEXT NOT NULL,
                volume_index  INTEGER NOT NULL,
                bytes_written INTEGER NOT NULL,
                temp_path     TEXT NOT NULL,
                start_offset  INTEGER NOT NULL DEFAULT 0,
                end_offset    INTEGER NOT NULL DEFAULT 0,
                verified      INTEGER NOT NULL DEFAULT 0,
                appended      INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (job_id, set_name, member_name, volume_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_archive_headers (
                job_id    INTEGER NOT NULL,
                set_name  TEXT NOT NULL,
                headers   BLOB NOT NULL,
                PRIMARY KEY (job_id, set_name)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_rar_volume_facts (
                job_id      INTEGER NOT NULL,
                set_name    TEXT NOT NULL,
                volume_index INTEGER NOT NULL,
                facts_blob  BLOB NOT NULL,
                PRIMARY KEY (job_id, set_name, volume_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_volume_status (
                job_id       INTEGER NOT NULL,
                set_name     TEXT NOT NULL,
                volume_index INTEGER NOT NULL,
                extracted    INTEGER NOT NULL DEFAULT 0,
                par2_clean   INTEGER NOT NULL DEFAULT 0,
                deleted      INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (job_id, set_name, volume_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_rar_verified_suspect (
                job_id       INTEGER NOT NULL,
                set_name     TEXT NOT NULL,
                volume_index INTEGER NOT NULL,
                PRIMARY KEY (job_id, set_name, volume_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS bandwidth_usage_minute_buckets (
                bucket_epoch_minute INTEGER PRIMARY KEY NOT NULL,
                payload_bytes       INTEGER NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS api_keys (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                key_hash     BLOB NOT NULL UNIQUE,
                scope        TEXT NOT NULL DEFAULT 'integration',
                created_at   INTEGER NOT NULL,
                last_used_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS auth_credentials (
                id            INTEGER PRIMARY KEY CHECK (id = 1),
                username      TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at    INTEGER NOT NULL,
                updated_at    INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS categories (
                id       INTEGER PRIMARY KEY NOT NULL,
                name     TEXT NOT NULL UNIQUE COLLATE NOCASE,
                dest_dir TEXT,
                aliases  TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS rss_feeds (
                id                 INTEGER PRIMARY KEY NOT NULL,
                name               TEXT NOT NULL,
                url                TEXT NOT NULL,
                enabled            INTEGER NOT NULL DEFAULT 1,
                poll_interval_secs INTEGER NOT NULL DEFAULT 900,
                username           TEXT,
                password           TEXT,
                default_category   TEXT,
                default_metadata   TEXT,
                etag               TEXT,
                last_modified      TEXT,
                last_polled_at     INTEGER,
                last_success_at    INTEGER,
                last_error         TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS rss_rules (
                id               INTEGER PRIMARY KEY NOT NULL,
                feed_id          INTEGER NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
                sort_order       INTEGER NOT NULL,
                enabled          INTEGER NOT NULL DEFAULT 1,
                action           TEXT NOT NULL,
                title_regex      TEXT,
                item_categories  TEXT,
                min_size_bytes   INTEGER,
                max_size_bytes   INTEGER,
                category_override TEXT,
                metadata         TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_rss_rules_feed_sort
                ON rss_rules(feed_id, sort_order, id);

            CREATE TABLE IF NOT EXISTS rss_seen_items (
                feed_id       INTEGER NOT NULL REFERENCES rss_feeds(id) ON DELETE CASCADE,
                item_id       TEXT NOT NULL,
                item_title    TEXT NOT NULL,
                published_at  INTEGER,
                size_bytes    INTEGER,
                decision      TEXT NOT NULL,
                seen_at       INTEGER NOT NULL,
                job_id        INTEGER,
                item_url      TEXT,
                error         TEXT,
                PRIMARY KEY (feed_id, item_id)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_rss_seen_seen_at
                ON rss_seen_items(seen_at);",
        )
        .map_err(|e| StateError::Database(e.to_string()))?;

        // Insert schema version if not present.
        let version: Option<i64> = conn
            .query_row("SELECT version FROM schema_version LIMIT 1", [], |row| {
                row.get(0)
            })
            .ok();

        match version {
            None => {
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    [SCHEMA_VERSION],
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(1) => {
                // v1→v3: tables already created above via IF NOT EXISTS.
                // VACUUM to enable auto_vacuum=INCREMENTAL retroactively.
                conn.execute_batch("VACUUM")
                    .map_err(|e| StateError::Database(e.to_string()))?;
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(2) | Some(3) => {
                // v2/v3→v11: append/priorities are added here; newer tables/columns are handled below.
                conn.execute_batch(
                    "ALTER TABLE active_extraction_chunks ADD COLUMN appended INTEGER NOT NULL DEFAULT 0;
                     ALTER TABLE servers ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;",
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(4) => {
                // v4→v11: append/priorities are added here; newer tables/columns are handled below.
                conn.execute_batch(
                    "ALTER TABLE active_extraction_chunks ADD COLUMN appended INTEGER NOT NULL DEFAULT 0;
                     ALTER TABLE servers ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;",
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(5) => {
                // v5→v11: server priority is added here; newer tables/columns are handled below.
                conn.execute_batch(
                    "ALTER TABLE servers ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(6) => {
                // v6→v11: RSS + categories + RAR facts + PAR2 file tables are created above.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(7) => {
                // v7→v11: categories + RAR facts + PAR2 file tables created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(8) => {
                // v8→v11: RAR volume facts + PAR2 file tables created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(9) => {
                // v9→v11: PAR2 file table created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(10) => {
                // v10→v12: newer active-state tables and history columns are created above.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(11) => {
                // v11→v12: active job normalization flag is added below; new active-state tables
                // are created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(12) => {
                // v12→v13: bandwidth usage ledger is created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(13) => {
                // v13→v14: public integration event log is created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(14) => {
                // v14→v15: compressed metrics scrape storage is created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(15) => {
                // v15→v16: active file progress floors are created above via IF NOT EXISTS.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(16) => {
                // v16→v17: active runtime restore columns are added below via ensure_column.
                conn.execute("UPDATE schema_version SET version = ?1", [SCHEMA_VERSION])
                    .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(StateError::Database(format!(
                    "unsupported schema version {v} (expected {SCHEMA_VERSION})"
                )));
            }
        }

        ensure_column(
            &conn,
            "job_history",
            "optional_recovery_bytes",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &conn,
            "job_history",
            "optional_recovery_downloaded_bytes",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &conn,
            "active_extraction_chunks",
            "start_offset",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &conn,
            "active_extraction_chunks",
            "end_offset",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(
            &conn,
            "active_jobs",
            "normalization_retried",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(&conn, "active_jobs", "queued_repair_at_epoch_ms", "REAL")?;
        ensure_column(&conn, "active_jobs", "queued_extract_at_epoch_ms", "REAL")?;
        ensure_column(&conn, "active_jobs", "paused_resume_status", "TEXT")?;
        ensure_column(&conn, "servers", "tls_ca_cert", "TEXT")?;

        Ok(())
    }

    /// Get the inner connection lock (for use by sub-modules).
    pub(crate) fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }

    /// Re-encrypt any plaintext passwords in the database.
    ///
    /// On upgrade from a version without encryption, passwords are stored as
    /// plaintext. This reads each one and re-writes it, which triggers the
    /// encrypt-on-write path. Idempotent — already-encrypted values pass through.
    pub fn migrate_plaintext_credentials(&self) -> Result<(), StateError> {
        use crate::persistence::encryption::{is_encrypted, maybe_encrypt};

        let Some(key) = self.encryption_key() else {
            return Ok(()); // no key set, nothing to do
        };

        let conn = self.conn();

        // Migrate server passwords
        let mut stmt = conn
            .prepare_cached("SELECT id, password FROM servers WHERE password IS NOT NULL")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let server_rows: Vec<(u32, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| StateError::Database(e.to_string()))?
            .filter_map(|r| r.ok())
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect();
        drop(stmt);

        for (id, plaintext) in &server_rows {
            let val: Option<String> = Some(plaintext.clone());
            if let Some(encrypted) = maybe_encrypt(Some(key), &val) {
                conn.execute(
                    "UPDATE servers SET password = ?1 WHERE id = ?2",
                    rusqlite::params![encrypted, id],
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
            }
        }
        if !server_rows.is_empty() {
            tracing::info!(
                count = server_rows.len(),
                "encrypted plaintext server passwords"
            );
        }

        // Migrate RSS feed passwords
        let mut stmt = conn
            .prepare_cached("SELECT id, password FROM rss_feeds WHERE password IS NOT NULL")
            .map_err(|e| StateError::Database(e.to_string()))?;
        let feed_rows: Vec<(u32, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| StateError::Database(e.to_string()))?
            .filter_map(|r| r.ok())
            .filter(|(_, pw)| !pw.is_empty() && !is_encrypted(pw))
            .collect();
        drop(stmt);

        for (id, plaintext) in &feed_rows {
            let val: Option<String> = Some(plaintext.clone());
            if let Some(encrypted) = maybe_encrypt(Some(key), &val) {
                conn.execute(
                    "UPDATE rss_feeds SET password = ?1 WHERE id = ?2",
                    rusqlite::params![encrypted, id],
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
            }
        }
        if !feed_rows.is_empty() {
            tracing::info!(
                count = feed_rows.len(),
                "encrypted plaintext RSS feed passwords"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
