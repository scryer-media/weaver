use super::*;

fn sample_history() -> JobHistoryRow {
    JobHistoryRow {
        job_id: 1,
        name: "test.nzb".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1_000_000,
        downloaded_bytes: 1_000_000,
        optional_recovery_bytes: 200_000,
        optional_recovery_downloaded_bytes: 50_000,
        failed_bytes: 0,
        health: 1000,
        category: Some("movies".to_string()),
        output_dir: Some("/tmp/output".to_string()),
        nzb_path: Some("/tmp/test.nzb".to_string()),
        created_at: 1700000000,
        completed_at: 1700001000,
        metadata: None,
    }
}

#[test]
fn insert_and_list() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();

    let entries = db.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "test.nzb");
    assert_eq!(entries[0].total_bytes, 1_000_000);
    assert_eq!(entries[0].optional_recovery_bytes, 200_000);
    assert_eq!(entries[0].optional_recovery_downloaded_bytes, 50_000);
    assert_eq!(db.get_job_history(1).unwrap().unwrap().name, "test.nzb");
}

#[test]
fn filter_by_status() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();

    let mut failed = sample_history();
    failed.job_id = 2;
    failed.status = "failed".to_string();
    failed.error_message = Some("missing volumes".to_string());
    db.insert_job_history(&failed).unwrap();

    let filter = HistoryFilter {
        status: Some("complete".to_string()),
        ..Default::default()
    };
    let entries = db.list_job_history(&filter).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].job_id, 1);
}

#[test]
fn delete_history() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();
    assert!(db.delete_job_history(1).unwrap());
    assert!(!db.delete_job_history(1).unwrap());
}

#[test]
fn max_job_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.max_job_id().unwrap(), 0);
}

#[test]
fn max_job_id_with_entries() {
    let db = Database::open_in_memory().unwrap();
    let mut entry = sample_history();
    entry.job_id = 42;
    db.insert_job_history(&entry).unwrap();
    assert_eq!(db.max_job_id().unwrap(), 42);
}

#[test]
fn list_with_limit_and_offset() {
    let db = Database::open_in_memory().unwrap();
    for i in 1..=5 {
        let mut entry = sample_history();
        entry.job_id = i;
        entry.completed_at = 1700000000 + i as i64;
        db.insert_job_history(&entry).unwrap();
    }

    let filter = HistoryFilter {
        limit: Some(2),
        offset: Some(1),
        ..Default::default()
    };
    let entries = db.list_job_history(&filter).unwrap();
    assert_eq!(entries.len(), 2);
    // Ordered by completed_at DESC, so job_ids: 5, 4, 3, 2, 1
    // offset=1, limit=2 → jobs 4, 3
    assert_eq!(entries[0].job_id, 4);
    assert_eq!(entries[1].job_id, 3);
}
