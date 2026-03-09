use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::StateError;

mod active;
mod api_keys;
mod config;
mod events;
mod history;
mod migration;

pub use active::{ActiveJob, CommittedSegment, ExtractionChunk, RecoveredJob};
pub use api_keys::ApiKeyRow;
pub use events::JobEvent;
pub use history::{HistoryFilter, JobHistoryRow};

const SCHEMA_VERSION: i64 = 4;

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
                supports_pipelining INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS job_history (
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

            CREATE TABLE IF NOT EXISTS active_extracted (
                job_id      INTEGER NOT NULL,
                member_name TEXT NOT NULL,
                output_path TEXT NOT NULL,
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
                verified      INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (job_id, set_name, member_name, volume_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS active_archive_headers (
                job_id    INTEGER NOT NULL,
                set_name  TEXT NOT NULL,
                headers   BLOB NOT NULL,
                PRIMARY KEY (job_id, set_name)
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

            CREATE TABLE IF NOT EXISTS api_keys (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                key_hash     BLOB NOT NULL UNIQUE,
                scope        TEXT NOT NULL DEFAULT 'integration',
                created_at   INTEGER NOT NULL,
                last_used_at INTEGER
            );",
        )
        .map_err(|e| StateError::Database(e.to_string()))?;

        // Insert schema version if not present.
        let version: Option<i64> = conn
            .query_row(
                "SELECT version FROM schema_version LIMIT 1",
                [],
                |row| row.get(0),
            )
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
                conn.execute(
                    "UPDATE schema_version SET version = ?1",
                    [SCHEMA_VERSION],
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(2) | Some(3) => {
                // v2/v3→v4: new tables created above via IF NOT EXISTS.
                conn.execute(
                    "UPDATE schema_version SET version = ?1",
                    [SCHEMA_VERSION],
                )
                .map_err(|e| StateError::Database(e.to_string()))?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(StateError::Database(format!(
                    "unsupported schema version {v} (expected {SCHEMA_VERSION})"
                )));
            }
        }

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
        assert_eq!(version, 4);
    }
}
