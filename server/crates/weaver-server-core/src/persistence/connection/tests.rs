use super::*;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::OptionalExtension;

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
fn open_applies_sqlite_memory_pragmas() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();
    let conn = db.conn();

    let cache_size: i64 = conn
        .query_row("PRAGMA cache_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cache_size, -16000);

    let mmap_size: i64 = conn
        .query_row("PRAGMA mmap_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mmap_size, 16 * 1024 * 1024);
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
        encryption_key: None,
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
        encryption_key: None,
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
        encryption_key: None,
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
        encryption_key: None,
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
        encryption_key: None,
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

#[test]
fn migrate_v14_adds_metrics_scrapes() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (14);",
    )
    .unwrap();

    let db = Database {
        conn: Arc::new(Mutex::new(conn)),
        encryption_key: None,
    };
    db.create_schema().unwrap();

    let conn = db.conn();
    let metrics_scrape_cols: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('metrics_scrapes')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(metrics_scrape_cols, 2);

    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}

#[test]
fn migrate_v15_adds_active_file_progress() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (15);",
    )
    .unwrap();

    let db = Database {
        conn: Arc::new(Mutex::new(conn)),
        encryption_key: None,
    };
    db.create_schema().unwrap();

    let conn = db.conn();
    let progress_cols: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('active_file_progress')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(progress_cols, 3);

    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}

#[test]
fn migrate_v16_adds_active_runtime_columns() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (16);
             CREATE TABLE active_jobs (
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
             );",
    )
    .unwrap();

    let db = Database {
        conn: Arc::new(Mutex::new(conn)),
        encryption_key: None,
    };
    db.create_schema().unwrap();

    let conn = db.conn();
    let runtime_cols: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('active_jobs')
                 WHERE name IN (
                    'queued_repair_at_epoch_ms',
                    'queued_extract_at_epoch_ms',
                    'paused_resume_status'
                 )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(runtime_cols, 3);

    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}

#[test]
fn migrate_v18_adds_two_lane_runtime_columns() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version (version) VALUES (18);
             CREATE TABLE active_jobs (
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
             );",
    )
    .unwrap();

    let db = Database {
        conn: Arc::new(Mutex::new(conn)),
        encryption_key: None,
    };
    db.create_schema().unwrap();

    let conn = db.conn();
    let runtime_cols: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('active_jobs')
                 WHERE name IN (
                    'download_state',
                    'post_state',
                    'run_state',
                    'paused_resume_download_state',
                    'paused_resume_post_state'
                 )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(runtime_cols, 5);

    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
}

#[test]
fn migrate_v19_purges_integration_events_and_prunes_old_metrics() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("weaver.db");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let metrics_cutoff_epoch_sec = now - 24 * 60 * 60;

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE schema_version (version INTEGER NOT NULL);
         INSERT INTO schema_version (version) VALUES (19);
         CREATE TABLE integration_events (
             id           INTEGER PRIMARY KEY AUTOINCREMENT,
             timestamp    INTEGER NOT NULL,
             kind         TEXT NOT NULL,
             item_id      INTEGER,
             payload_json TEXT NOT NULL
         );
         CREATE TABLE metrics_scrapes (
             scraped_at_epoch_sec INTEGER PRIMARY KEY NOT NULL,
             body_zstd            BLOB NOT NULL
         ) WITHOUT ROWID;
         CREATE TABLE job_history (
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
         );",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            now * 1000,
            "ITEM_CREATED",
            7_i64,
            "{\"kind\":\"ITEM_CREATED\"}"
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            now * 1000 + 1,
            "ITEM_PROGRESS",
            7_i64,
            "{\"kind\":\"ITEM_PROGRESS\"}"
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO metrics_scrapes (scraped_at_epoch_sec, body_zstd) VALUES (?1, ?2)",
        rusqlite::params![metrics_cutoff_epoch_sec - 60, vec![1_u8]],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO metrics_scrapes (scraped_at_epoch_sec, body_zstd) VALUES (?1, ?2)",
        rusqlite::params![metrics_cutoff_epoch_sec + 60, vec![2_u8]],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO job_history (
             job_id, name, status, error_message, total_bytes, downloaded_bytes,
             optional_recovery_bytes, optional_recovery_downloaded_bytes,
             failed_bytes, health, category, output_dir, nzb_path, created_at,
             completed_at, metadata
         ) VALUES (
             ?1, ?2, ?3, NULL, 0, 0, 0, 0, 0, 1000, NULL, NULL, NULL, ?4, ?5, NULL
         )",
        rusqlite::params![42_i64, "retained", "complete", now - 120, now - 60],
    )
    .unwrap();
    drop(conn);

    let db = Database {
        conn: Arc::new(Mutex::new(Connection::open(&db_path).unwrap())),
        encryption_key: None,
    };
    db.create_schema().unwrap();

    let conn = db.conn();
    let version: i64 = conn
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SCHEMA_VERSION);

    let integration_events: i64 = conn
        .query_row("SELECT COUNT(*) FROM integration_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(integration_events, 0);

    let retained_metrics: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM metrics_scrapes WHERE scraped_at_epoch_sec >= ?1",
            [metrics_cutoff_epoch_sec],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_metrics, 1);

    let stale_metrics: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM metrics_scrapes WHERE scraped_at_epoch_sec < ?1",
            [metrics_cutoff_epoch_sec],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stale_metrics, 0);

    let preserved_history: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM job_history WHERE job_id = 42",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(preserved_history, 1);

    let integration_sequence: Option<i64> = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'integration_events'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert!(integration_sequence.is_none());
}
