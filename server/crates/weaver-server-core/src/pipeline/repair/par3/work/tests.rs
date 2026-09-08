use super::*;

fn carrier(root: &std::path::Path) -> PathBuf {
    let path = root.join("set.par3");
    std::fs::write(&path, include_bytes!("../../backend/fixtures/set.par3")).unwrap();
    path
}

async fn next(coordinator: &mut Coordinator) -> WorkDone {
    tokio::time::timeout(std::time::Duration::from_secs(10), coordinator.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn workers_return_retained_state_and_serialize_carriers() {
    let root = tempfile::tempdir().unwrap();
    let path = carrier(root.path());
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), path.clone())
        .unwrap();
    coordinator.enqueue(JobId(2), SourceId(0), path).unwrap();
    coordinator.dispatch().unwrap();
    coordinator.dispatch().unwrap();
    assert_eq!(coordinator.in_flight.len(), 1);
    assert!(coordinator.jobs[&JobId(1)].runtime.is_none());
    assert!(coordinator.jobs[&JobId(2)].runtime.is_some());
    let done = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(done), Some(JobId(1)));
    assert_eq!(
        coordinator.jobs[&JobId(1)]
            .runtime
            .as_ref()
            .unwrap()
            .sets
            .len(),
        1
    );
    assert!(!coordinator.has_work(JobId(1)));
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(done), Some(JobId(2)));
    assert!(coordinator.in_flight.is_empty());
}

#[tokio::test]
async fn forgetting_and_recreating_job_cannot_admit_stale_state_or_extra_workers() {
    let root = tempfile::tempdir().unwrap();
    let path = carrier(root.path());
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), path.clone())
        .unwrap();
    coordinator.dispatch().unwrap();
    let token = coordinator.in_flight.values().next().unwrap().1.clone();
    coordinator.forget(JobId(1));
    assert!(matches!(token.check(), Err(EngineError::Cancelled)));
    coordinator.enqueue(JobId(1), SourceId(0), path).unwrap();
    coordinator.dispatch().unwrap();
    assert_eq!(coordinator.in_flight.len(), 1);
    assert_eq!(coordinator.jobs[&JobId(1)].ticket, None);
    let stale = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(stale), None);
    assert!(
        coordinator.jobs[&JobId(1)]
            .runtime
            .as_ref()
            .unwrap()
            .sets
            .is_empty()
    );
    assert!(coordinator.has_work(JobId(1)));
    coordinator.dispatch().unwrap();
    let fresh = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(fresh), Some(JobId(1)));
    assert_eq!(
        coordinator.jobs[&JobId(1)]
            .runtime
            .as_ref()
            .unwrap()
            .sets
            .len(),
        1
    );
}

#[tokio::test]
async fn backing_io_errors_remain_typed_after_worker_handoff() {
    let root = tempfile::tempdir().unwrap();
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), root.path().to_path_buf())
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(done), Some(JobId(1)));
    assert!(
        matches!(coordinator.jobs[&JobId(1)].errors.get(&SourceId(0)), Some(EngineError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
}

#[test]
fn queued_carriers_and_jobs_have_explicit_limits_and_deduplicate_replays() {
    let mut coordinator = Coordinator::default();
    for id in 0..MAX_PENDING {
        coordinator
            .enqueue(
                JobId(0),
                SourceId(id as u64),
                PathBuf::from("candidate.par3"),
            )
            .unwrap();
    }
    coordinator
        .enqueue(JobId(0), SourceId(0), PathBuf::from("renamed.par3"))
        .unwrap();
    assert_eq!(coordinator.jobs[&JobId(0)].pending.len(), MAX_PENDING);
    assert!(matches!(
        coordinator.enqueue(JobId(0), SourceId(MAX_PENDING as u64), PathBuf::new()),
        Err(EngineError::ResourceLimit(_))
    ));
    coordinator.forget(JobId(0));
    for id in 0..MAX_JOBS {
        coordinator
            .enqueue(JobId(id as u64), SourceId(0), PathBuf::new())
            .unwrap();
    }
    assert!(matches!(
        coordinator.enqueue(JobId(MAX_JOBS as u64), SourceId(0), PathBuf::new()),
        Err(EngineError::ResourceLimit(_))
    ));
}
