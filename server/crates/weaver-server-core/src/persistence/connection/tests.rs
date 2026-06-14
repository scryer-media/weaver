use super::*;

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::persistence::database_target::DatabaseTarget;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

fn fetch_i64(db: &Database, sql: &'static str, args: Vec<SqlArg>) -> i64 {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.i64_at(0)
    })
    .unwrap()
}

fn fetch_text(db: &Database, sql: &'static str, args: Vec<SqlArg>) -> String {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        let row = SqlRuntime::fetch_optional(datastore.read_exec(), sql, &args)
            .await?
            .ok_or_else(|| StateError::Database(format!("query returned no rows: {sql}")))?;
        row.text("value")
    })
    .unwrap()
}

fn execute(db: &Database, sql: &'static str, args: Vec<SqlArg>) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.read_exec(), sql, &args).await?;
        Ok(())
    })
    .unwrap()
}

fn current_schema_version() -> i64 {
    crate::schema_migrations::embedded_catalog()
        .unwrap()
        .migrations
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap()
}

#[test]
fn open_in_memory_creates_schema() {
    let db = Database::open_in_memory().unwrap();
    assert!(db.is_empty().unwrap());
}

#[test]
fn open_in_memory_stamps_sqlx_migration_ledger() {
    let db = Database::open_in_memory().unwrap();
    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());
}

#[test]
fn schema_version_is_set() {
    let db = Database::open_in_memory().unwrap();
    let version = fetch_i64(&db, "SELECT version FROM schema_version", vec![]);
    assert_eq!(version, current_schema_version());
}

#[test]
fn open_applies_sqlite_file_pragmas() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();

    let cache_size = fetch_i64(&db, "PRAGMA cache_size", vec![]);
    assert_eq!(cache_size, -16000);

    let mmap_size = fetch_i64(&db, "PRAGMA mmap_size", vec![]);
    assert_eq!(mmap_size, 16 * 1024 * 1024);
}

#[test]
fn open_file_database_stamps_sqlx_migration_ledger() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();
    let catalog = crate::schema_migrations::embedded_catalog().unwrap();

    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());

    let migration_count = fetch_i64(
        &db,
        "SELECT COUNT(*) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(migration_count, catalog.migrations.len() as i64);

    let checksum_algo = fetch_text(
        &db,
        "SELECT checksum_algo AS value FROM _sqlx_migrations WHERE version = {}",
        vec![SqlArg::I64(current_schema_version())],
    );
    assert_eq!(checksum_algo, "blake3");
}

#[test]
fn reserve_next_job_id_remains_monotonic_across_reopen_without_history_rows() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("weaver.db");

    {
        let db = Database::open(&db_path).unwrap();
        assert_eq!(db.reserve_next_job_id().unwrap().0, 10_000);

        execute(&db, "DELETE FROM active_jobs", vec![]);
        execute(&db, "DELETE FROM job_history", vec![]);
    }

    let db = Database::open(&db_path).unwrap();
    assert_eq!(db.reserve_next_job_id().unwrap().0, 10_001);

    let persisted_next = fetch_text(
        &db,
        "SELECT value FROM settings WHERE key = 'next_job_id'",
        vec![],
    );
    assert_eq!(persisted_next, "10002");
}

#[tokio::test]
async fn flush_write_queue_succeeds_without_raw_sqlite_connection() {
    let db = Database::open_in_memory().unwrap();
    db.flush_write_queue().await.unwrap();
}

#[tokio::test]
async fn postgres_runtime_smoke_when_configured() {
    let Ok(base_url) = std::env::var("WEAVER_TEST_POSTGRES_URL") else {
        eprintln!("skipping postgres smoke; WEAVER_TEST_POSTGRES_URL is not set");
        return;
    };
    if base_url.trim().is_empty() {
        eprintln!("skipping postgres smoke; WEAVER_TEST_POSTGRES_URL is empty");
        return;
    }

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("weaver_test_{}_{}", std::process::id(), suffix);
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&base_url)
        .await
        .unwrap();
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&admin_pool)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin_pool)
        .await
        .unwrap();

    let target_url = postgres_url_for_schema(&base_url, &schema);
    let db = Database::open_target(DatabaseTarget::PostgresUrl(target_url)).unwrap();

    let latest_version = fetch_i64(
        &db,
        "SELECT MAX(version) AS value FROM _sqlx_migrations",
        vec![],
    );
    assert_eq!(latest_version, current_schema_version());

    db.set_setting("postgres_smoke", "ok").unwrap();
    assert_eq!(
        db.get_setting("postgres_smoke").unwrap(),
        Some("ok".to_string())
    );
    assert_eq!(db.reserve_next_job_id().unwrap().0, 10_000);

    let api_key_id = db
        .insert_api_key("integration", &[0x31_u8; 32], "integration")
        .unwrap();
    assert!(api_key_id > 0);

    let job_id = crate::jobs::ids::JobId(42);
    db.create_active_job(&ActiveJob {
        job_id,
        nzb_hash: [0xA5; 32],
        nzb_path: PathBuf::from("/tmp/postgres-smoke.nzb"),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"/>"#,
        )
        .unwrap(),
        output_dir: PathBuf::from("/tmp/postgres-smoke"),
        created_at: 1_700_000_000,
        category: Some("smoke".to_string()),
        metadata: vec![("engine".to_string(), "postgres".to_string())],
    })
    .unwrap();
    db.commit_segments(&[CommittedSegment {
        job_id,
        file_index: 0,
        segment_number: 1,
        file_offset: 0,
        decoded_size: 123,
        crc32: 0x1234,
    }])
    .unwrap();
    assert_eq!(db.load_active_jobs().unwrap().len(), 1);

    db.archive_job(
        job_id,
        &JobHistoryRow {
            job_id: job_id.0,
            job_hash: Some(vec![0xA5; 32]),
            name: "postgres-smoke".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 123,
            downloaded_bytes: 123,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: Some("smoke".to_string()),
            output_dir: Some("/tmp/postgres-smoke".to_string()),
            nzb_path: Some("/tmp/postgres-smoke.nzb".to_string()),
            created_at: 1_700_000_000,
            completed_at: 1_700_000_100,
            metadata: Some("[[\"engine\",\"postgres\"]]".to_string()),
            last_diagnostic_id: None,
            last_diagnostic_uploaded_at_epoch_ms: None,
        },
    )
    .unwrap();
    assert!(db.load_active_jobs().unwrap().is_empty());
    assert!(db.get_job_history(job_id.0).unwrap().is_some());

    drop(db);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin_pool)
        .await
        .unwrap();
    admin_pool.close().await;
}

fn postgres_url_for_schema(base_url: &str, schema: &str) -> String {
    let separator = if base_url.contains('?') { '&' } else { '?' };
    format!("{base_url}{separator}options=-csearch_path%3D{schema}")
}
