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
fn batch_insert_events_preserves_order_across_chunks() {
    let db = Database::open_in_memory().unwrap();
    let events = (0..250)
        .map(|index| JobEvent {
            job_id: 1,
            timestamp: 1000 + index,
            kind: format!("EVENT_{index:03}"),
            message: format!("message {index}"),
            file_id: Some(format!("1:{index}")),
        })
        .collect::<Vec<_>>();

    db.insert_job_events(&events).unwrap();

    let loaded = db.get_job_events(1).unwrap();
    assert_eq!(loaded.len(), events.len());
    for (index, event) in loaded.iter().enumerate() {
        assert_eq!(event.kind, format!("EVENT_{index:03}"));
        assert_eq!(
            event.file_id.as_deref(),
            Some(format!("1:{index}").as_str())
        );
    }
}

#[test]
#[ignore = "performance guard; run explicitly when comparing DB write throughput"]
fn perf_insert_10k_job_events() {
    let db = Database::open_in_memory().unwrap();
    let events = (0..10_000)
        .map(|index| JobEvent {
            job_id: 1,
            timestamp: 1000 + index,
            kind: "PROGRESS".to_string(),
            message: format!("message {index}"),
            file_id: Some(format!("1:{index}")),
        })
        .collect::<Vec<_>>();

    let started = std::time::Instant::now();
    db.insert_job_events(&events).unwrap();
    let elapsed = started.elapsed();

    eprintln!(
        "insert_job_events: {} rows in {:?} ({:.0} rows/sec)",
        events.len(),
        elapsed,
        events.len() as f64 / elapsed.as_secs_f64()
    );
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
