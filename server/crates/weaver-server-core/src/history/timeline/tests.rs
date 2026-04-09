use super::*;

#[test]
fn insert_and_load_events() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_event(1, 1000, "JOB_CREATED", "test: 5 files", None)
        .unwrap();
    db.insert_job_event(
        1,
        1001,
        "FILE_COMPLETE",
        "data.rar: 1000 bytes",
        Some("1:0"),
    )
    .unwrap();
    db.insert_job_event(2, 1002, "JOB_CREATED", "other job", None)
        .unwrap();

    let events = db.get_job_events(1).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "JOB_CREATED");
    assert_eq!(events[1].kind, "FILE_COMPLETE");
    assert_eq!(events[1].file_id.as_deref(), Some("1:0"));

    // Job 2 has its own events.
    let events2 = db.get_job_events(2).unwrap();
    assert_eq!(events2.len(), 1);
}

#[test]
fn batch_insert_events() {
    let db = Database::open_in_memory().unwrap();
    let events = vec![
        JobEvent {
            job_id: 1,
            timestamp: 1000,
            kind: "JOB_CREATED".to_string(),
            message: "created".to_string(),
            file_id: None,
        },
        JobEvent {
            job_id: 1,
            timestamp: 1001,
            kind: "FILE_COMPLETE".to_string(),
            message: "done".to_string(),
            file_id: Some("1:0".to_string()),
        },
    ];
    db.insert_job_events(&events).unwrap();
    assert_eq!(db.get_job_events(1).unwrap().len(), 2);
}

#[test]
fn delete_events() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_event(1, 1000, "JOB_CREATED", "test", None)
        .unwrap();
    db.insert_job_event(1, 1001, "FILE_COMPLETE", "done", None)
        .unwrap();
    db.delete_job_events(1).unwrap();
    assert!(db.get_job_events(1).unwrap().is_empty());
}
