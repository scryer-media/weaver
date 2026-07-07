use super::*;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{ActiveJob, JobId};

fn maintenance_snapshot(db: &Database) -> DbMaintenanceSnapshot {
    let datastore = db.datastore();
    db.run_sql_blocking(async move { DbMaintenanceSnapshot::read(&datastore).await })
        .unwrap()
}

fn write_last_full_vacuum_success_epoch_secs_for_test(db: &Database, epoch_secs: i64) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        write_last_full_vacuum_success_epoch_secs(&datastore, epoch_secs).await
    })
    .unwrap();
}

fn low_threshold_options() -> DbMaintenanceOptions {
    DbMaintenanceOptions {
        reclaim_threshold_bytes: 4096,
        freelist_ratio_threshold: 0.01,
        incremental_batch_pages: 32,
        max_incremental_pages: 4096,
        max_vacuum_duration: Duration::from_secs(5),
        wal_truncate_threshold_bytes: u64::MAX,
        full_vacuum_min_interval: Duration::from_secs(24 * 60 * 60),
    }
}

fn open_file_database() -> (tempfile::TempDir, Database) {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("weaver.db")).unwrap();
    (temp, db)
}

fn create_free_pages(db: &Database, rows: usize) {
    let blob = vec![0xA5_u8; 8192];
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "CREATE TABLE IF NOT EXISTS maintenance_blob (
                id INTEGER PRIMARY KEY,
                body BLOB NOT NULL
            )",
            &[],
        )
        .await?;

        SqlRuntime::run_in_transaction(&datastore, "maintenance_test_blob_insert", |tx| {
            let blob = blob.clone();
            Box::pin(async move {
                for id in 0..rows {
                    tx.execute(
                        "INSERT INTO maintenance_blob (id, body) VALUES ({}, {})",
                        &[SqlArg::I64(id as i64), SqlArg::Bytes(blob.clone())],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await?;

        SqlRuntime::execute(datastore.read_exec(), "DELETE FROM maintenance_blob", &[]).await?;
        let _ = SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "PRAGMA wal_checkpoint(TRUNCATE)",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

fn execute_sql(db: &Database, sql: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.read_exec(), sql, &[]).await?;
        Ok(())
    })
    .unwrap();
}

fn sample_active_job(id: u64) -> ActiveJob {
    ActiveJob {
        job_id: JobId(id),
        nzb_hash: [0xBC; 32],
        nzb_path: PathBuf::from(format!("/tmp/job-{id}.nzb")),
        nzb_zstd: Vec::new(),
        output_dir: PathBuf::from(format!("/tmp/job-{id}")),
        created_at: 1_700_000_000,
        category: None,
        metadata: Vec::new(),
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

#[test]
fn maintenance_reclaims_incremental_free_pages() {
    let db = Database::open_in_memory().unwrap();
    create_free_pages(&db, 256);
    db.create_active_job(&sample_active_job(1)).unwrap();

    let before = maintenance_snapshot(&db);
    assert!(before.freelist_count > 0);

    let report = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert!(report.incremental_vacuum_ran);
    assert_eq!(
        report.full_vacuum_decision,
        FullVacuumDecision::SkippedActiveJobs
    );
    assert!(report.after.freelist_count < before.freelist_count);
    assert!(report.after.page_count < before.page_count);
    assert!(report.reclaimed_bytes_estimate > 0);
}

#[test]
fn idle_maintenance_runs_full_vacuum_for_large_freelist() {
    let (_temp, db) = open_file_database();
    create_free_pages(&db, 256);

    let before = maintenance_snapshot(&db);
    assert!(before.freelist_count > 0);
    assert!(before.db_size_bytes.unwrap_or(0) > 0);

    let report = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert_eq!(report.active_job_count, 0);
    assert!(report.full_vacuum_ran);
    assert_eq!(report.full_vacuum_decision, FullVacuumDecision::Ran);
    assert!(!report.incremental_vacuum_ran);
    assert!(report.after.freelist_count < before.freelist_count);
    assert!(report.after.page_count < before.page_count);
    assert!(report.after.db_size_bytes.unwrap_or(u64::MAX) < before.db_size_bytes.unwrap());
}

#[test]
fn maintenance_skips_vacuum_below_threshold() {
    let db = Database::open_in_memory().unwrap();
    let options = DbMaintenanceOptions {
        reclaim_threshold_bytes: u64::MAX,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(!report.incremental_vacuum_ran);
    assert!(!report.full_vacuum_ran);
    assert_eq!(report.full_vacuum_decision, FullVacuumDecision::NotNeeded);
    assert_eq!(report.vacuum_iterations, 0);
}

#[test]
fn maintenance_reports_budget_limited_pass() {
    let db = Database::open_in_memory().unwrap();
    create_free_pages(&db, 256);
    db.create_active_job(&sample_active_job(2)).unwrap();
    let options = DbMaintenanceOptions {
        incremental_batch_pages: 1,
        max_incremental_pages: 1,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(report.incremental_vacuum_ran);
    assert_eq!(
        report.full_vacuum_decision,
        FullVacuumDecision::SkippedActiveJobs
    );
    assert_eq!(report.vacuum_iterations, 1);
    assert!(report.budget_limited);
}

#[test]
fn active_job_with_large_freelist_skips_full_vacuum_and_uses_incremental() {
    let (_temp, db) = open_file_database();
    create_free_pages(&db, 256);
    db.create_active_job(&sample_active_job(3)).unwrap();

    let report = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert_eq!(report.active_job_count, 1);
    assert!(!report.full_vacuum_ran);
    assert_eq!(
        report.full_vacuum_decision,
        FullVacuumDecision::SkippedActiveJobs
    );
    assert!(report.incremental_vacuum_ran);
}

#[test]
fn second_idle_pass_within_interval_skips_full_vacuum() {
    let (_temp, db) = open_file_database();
    create_free_pages(&db, 256);
    let first = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();
    assert!(first.full_vacuum_ran);

    create_free_pages(&db, 256);
    let second = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert!(!second.full_vacuum_ran);
    assert_eq!(
        second.full_vacuum_decision,
        FullVacuumDecision::SkippedRecentSuccess
    );
    assert!(second.full_vacuum_last_success_epoch_secs.is_some());
    assert!(!second.incremental_vacuum_ran);
}

#[test]
fn idle_pass_after_interval_can_full_vacuum_again() {
    let (_temp, db) = open_file_database();
    create_free_pages(&db, 256);
    let first = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();
    assert!(first.full_vacuum_ran);

    write_last_full_vacuum_success_epoch_secs_for_test(
        &db,
        current_epoch_secs() - (24 * 60 * 60) - 1,
    );
    create_free_pages(&db, 256);

    let second = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert!(second.full_vacuum_ran);
    assert_eq!(second.full_vacuum_decision, FullVacuumDecision::Ran);
}

#[test]
fn incremental_budget_uses_observed_reclaimed_pages() {
    let (_temp, db) = open_file_database();
    create_free_pages(&db, 256);
    db.create_active_job(&sample_active_job(4)).unwrap();
    let options = DbMaintenanceOptions {
        reclaim_threshold_bytes: 0,
        incremental_batch_pages: 4096,
        max_incremental_pages: 3,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(report.incremental_vacuum_ran);
    assert!(report.vacuum_iterations >= 1);
    assert!(
        report.reclaimed_pages_estimate <= options.max_incremental_pages,
        "incremental vacuum should respect the observed reclaimed-page budget"
    );
    assert!(report.budget_limited);
}

#[test]
fn wal_checkpoint_truncates_only_without_active_jobs() {
    let db = Database::open_in_memory().unwrap();
    execute_sql(
        &db,
        "CREATE TABLE wal_checkpoint_test (id INTEGER PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &db,
        "INSERT INTO wal_checkpoint_test (body) VALUES ('wal bytes')",
    );
    let options = DbMaintenanceOptions {
        wal_truncate_threshold_bytes: 0,
        reclaim_threshold_bytes: u64::MAX,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(report.wal_truncate_ran);

    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_active_job(7)).unwrap();
    execute_sql(
        &db,
        "CREATE TABLE wal_checkpoint_active_test (id INTEGER PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &db,
        "INSERT INTO wal_checkpoint_active_test (body) VALUES ('wal bytes')",
    );

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert_eq!(report.active_job_count, 1);
    assert!(!report.wal_truncate_ran);
}

#[test]
fn postgres_analyze_tables_are_well_formed() {
    // The list must be non-empty and free of duplicates, and every hot table the
    // pass claims to cover must be present. Postgres ANALYZE builds statements
    // straight from these identifiers, so they must be stable, known names.
    assert!(!POSTGRES_ANALYZE_TABLES.is_empty());

    let unique: std::collections::HashSet<_> = POSTGRES_ANALYZE_TABLES.iter().collect();
    assert_eq!(
        unique.len(),
        POSTGRES_ANALYZE_TABLES.len(),
        "duplicate table in POSTGRES_ANALYZE_TABLES"
    );

    for expected in [
        "job_events",
        "job_history",
        "job_history_attributes",
        "active_file_progress",
        "active_files",
        "active_extracted",
        "async_operation_targets",
        "integration_events",
        "metrics_history_chunks",
    ] {
        assert!(
            POSTGRES_ANALYZE_TABLES.contains(&expected),
            "missing hot table {expected}"
        );
    }

    // VACUUM is owned by autovacuum; the analyze pass must never emit it.
    for table in POSTGRES_ANALYZE_TABLES {
        assert!(!table.contains(' '), "table name must be a bare identifier");
    }
}

#[test]
fn postgres_maintenance_pass_rejects_non_postgres_datastore() {
    // The engine gate must refuse to run the Postgres pass against sqlite, the
    // mirror of the sqlite pass's own guard.
    let db = Database::open_in_memory().unwrap();
    let error = db.run_postgres_maintenance_pass().unwrap_err();
    assert!(matches!(error, StateError::Database(_)));
}
