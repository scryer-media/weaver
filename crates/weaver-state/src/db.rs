use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::StateError;

mod active;
mod api_keys;
mod backup;
mod bandwidth;
mod config;
mod events;
mod history;
mod migration;
mod rss;

pub use active::{ActiveJob, ActivePar2File, CommittedSegment, ExtractionChunk, RecoveredJob};
pub use api_keys::ApiKeyRow;
pub use backup::StableStateExport;
pub use events::JobEvent;
pub use history::{HistoryFilter, JobHistoryRow};
pub use rss::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};

const SCHEMA_VERSION: i64 = 13;

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
             PRAGMA auto_vacuum=INCREMENTAL;",
        )
        .map_err(|e| StateError::Database(e.to_string()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema()?;
        Ok(db)
    }

    /// Open an in-memory database (for tests).
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self, StateError> {
        let conn = Connection::open_in_memory().map_err(|e| StateError::Database(e.to_string()))?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema()?;
        Ok(db)
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

        Ok(())
    }

    /// Get the inner connection lock (for use by sub-modules).
    pub(crate) fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn open_in_memory_creates_schema() {
        let db = Database::open_in_memory().unwrap();
        assert!(db.is_empty().unwrap());
    }

    #[test]
    fn schema_version_is_set() {
        let db = Database::open_in_memory().unwrap();
        let conn = db.conn();
        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_v3_creates_extraction_chunks_with_appended_once() {
        let conn = Connection::open_in_memory().unwrap();
        // Create tables as they existed at v3 (no appended, no priority).
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (3);
             CREATE TABLE servers (
                 id                  INTEGER PRIMARY KEY NOT NULL,
                 host                TEXT NOT NULL,
                 port                INTEGER NOT NULL,
                 tls                 INTEGER NOT NULL DEFAULT 1,
                 username            TEXT,
                 password            TEXT,
                 connections         INTEGER NOT NULL DEFAULT 10,
                 active              INTEGER NOT NULL DEFAULT 1,
                 supports_pipelining INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE active_extraction_chunks (
                 job_id        INTEGER NOT NULL,
                 set_name      TEXT NOT NULL,
                 member_name   TEXT NOT NULL,
                 volume_index  INTEGER NOT NULL,
                 bytes_written INTEGER NOT NULL,
                 temp_path     TEXT NOT NULL,
                 verified      INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (job_id, set_name, member_name, volume_index)
             ) WITHOUT ROWID;",
        )
        .unwrap();

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema().unwrap();

        let conn = db.conn();
        let appended_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('active_extraction_chunks')
                 WHERE name = 'appended'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(appended_cols, 1);

        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_v6_creates_rss_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (6);",
        )
        .unwrap();

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema().unwrap();

        let conn = db.conn();
        let feed_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('rss_feeds')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let rule_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('rss_rules')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let seen_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('rss_seen_items')",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert!(feed_cols > 0);
        assert!(rule_cols > 0);
        assert!(seen_cols > 0);

        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_v10_adds_optional_recovery_history_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (10);
             CREATE TABLE job_history (
                 job_id           INTEGER PRIMARY KEY NOT NULL,
                 name             TEXT NOT NULL,
                 status           TEXT NOT NULL,
                 error_message    TEXT,
                 total_bytes      INTEGER NOT NULL DEFAULT 0,
                 downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                 failed_bytes     INTEGER NOT NULL DEFAULT 0,
                 health           INTEGER NOT NULL DEFAULT 1000,
                 category         TEXT,
                 output_dir       TEXT,
                 nzb_path         TEXT,
                 created_at       INTEGER NOT NULL,
                 completed_at     INTEGER NOT NULL,
                 metadata         TEXT
             );",
        )
        .unwrap();

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema().unwrap();

        let conn = db.conn();
        let optional_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('job_history')
                 WHERE name IN ('optional_recovery_bytes', 'optional_recovery_downloaded_bytes')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(optional_cols, 2);

        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_v11_adds_restart_runtime_state() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (11);
             CREATE TABLE active_jobs (
                 job_id       INTEGER PRIMARY KEY NOT NULL,
                 nzb_hash     BLOB NOT NULL,
                 nzb_path     TEXT NOT NULL,
                 output_dir   TEXT NOT NULL,
                 status       TEXT NOT NULL DEFAULT 'downloading',
                 error        TEXT,
                 created_at   INTEGER NOT NULL,
                 category     TEXT,
                 metadata     TEXT
             );",
        )
        .unwrap();

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema().unwrap();

        let conn = db.conn();
        let normalization_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('active_jobs')
                 WHERE name = 'normalization_retried'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(normalization_cols, 1);

        let failed_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('active_failed_extractions')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let suspect_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('active_rar_verified_suspect')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(failed_cols > 0);
        assert!(suspect_cols > 0);

        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_v12_adds_bandwidth_usage_ledger() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (12);",
        )
        .unwrap();

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.create_schema().unwrap();

        let conn = db.conn();
        let bucket_cols: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('bandwidth_usage_minute_buckets')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(bucket_cols > 0);

        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }
}
