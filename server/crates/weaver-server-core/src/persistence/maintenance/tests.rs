use super::*;
use crate::{ActiveJob, JobId};

fn low_threshold_options() -> DbMaintenanceOptions {
    DbMaintenanceOptions {
        reclaim_threshold_bytes: 4096,
        freelist_ratio_threshold: 0.01,
        incremental_batch_pages: 32,
        max_incremental_pages: 4096,
        max_vacuum_duration: Duration::from_secs(5),
        wal_truncate_threshold_bytes: u64::MAX,
    }
}

fn create_free_pages(db: &Database, rows: usize) {
    let blob = vec![0xA5_u8; 8192];
    let conn = db.conn();
    conn.execute_batch(
        "CREATE TABLE maintenance_blob (
            id INTEGER PRIMARY KEY,
            body BLOB NOT NULL
        );",
    )
    .unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        for id in 0..rows {
            tx.execute(
                "INSERT INTO maintenance_blob (id, body) VALUES (?1, ?2)",
                rusqlite::params![id as i64, blob],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    conn.execute("DELETE FROM maintenance_blob", []).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
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
    }
}

#[test]
fn maintenance_reclaims_incremental_free_pages() {
    let db = Database::open_in_memory().unwrap();
    create_free_pages(&db, 256);

    let before = {
        let conn = db.conn();
        DbMaintenanceSnapshot::read(&conn).unwrap()
    };
    assert!(before.freelist_count > 0);

    let report = db
        .run_sqlite_maintenance_pass(low_threshold_options())
        .unwrap();

    assert!(report.incremental_vacuum_ran);
    assert!(report.after.freelist_count < before.freelist_count);
    assert!(report.after.page_count < before.page_count);
    assert!(report.reclaimed_bytes_estimate > 0);
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
    assert_eq!(report.vacuum_iterations, 0);
}

#[test]
fn maintenance_reports_budget_limited_pass() {
    let db = Database::open_in_memory().unwrap();
    create_free_pages(&db, 256);
    let options = DbMaintenanceOptions {
        incremental_batch_pages: 1,
        max_incremental_pages: 1,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(report.incremental_vacuum_ran);
    assert_eq!(report.vacuum_iterations, 1);
    assert!(report.budget_limited);
}

#[test]
fn wal_checkpoint_truncates_only_without_active_jobs() {
    let db = Database::open_in_memory().unwrap();
    {
        let conn = db.conn();
        conn.execute_batch(
            "CREATE TABLE wal_checkpoint_test (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO wal_checkpoint_test (body) VALUES ('wal bytes');",
        )
        .unwrap();
    }
    let options = DbMaintenanceOptions {
        wal_truncate_threshold_bytes: 0,
        reclaim_threshold_bytes: u64::MAX,
        ..low_threshold_options()
    };

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert!(report.wal_truncate_ran);

    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&sample_active_job(7)).unwrap();
    {
        let conn = db.conn();
        conn.execute_batch(
            "CREATE TABLE wal_checkpoint_active_test (id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO wal_checkpoint_active_test (body) VALUES ('wal bytes');",
        )
        .unwrap();
    }

    let report = db.run_sqlite_maintenance_pass(options).unwrap();

    assert_eq!(report.active_job_count, 1);
    assert!(!report.wal_truncate_ran);
}
