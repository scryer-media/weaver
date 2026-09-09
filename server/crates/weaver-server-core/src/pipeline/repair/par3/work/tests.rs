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
async fn source_write_fences_a_finished_but_unsettled_worker() {
    let root = tempfile::tempdir().unwrap();
    let path = carrier(root.path());
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), path.clone())
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    assert!(done.result.is_ok());
    assert!(
        done.runtime
            .as_ref()
            .unwrap()
            .sets
            .values()
            .all(|set| set.view.is_some())
    );
    coordinator
        .invalidate_source(JobId(1), SourceId(0))
        .unwrap();
    assert_eq!(
        coordinator.in_flight.len(),
        1,
        "write invalidation cannot release worker capacity"
    );
    coordinator.settle(done);
    assert!(coordinator.in_flight.is_empty());
    assert!(coordinator.has_work(JobId(1)));
    assert_eq!(coordinator.assessments(JobId(1)).count(), 0);
    assert_eq!(coordinator.dirty_sources(JobId(1)), [SourceId(0)]);
    coordinator.enqueue(JobId(1), SourceId(0), path).unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    assert!(!coordinator.has_work(JobId(1)));
    assert_eq!(coordinator.assessments(JobId(1)).count(), 1);
}

#[test]
fn writes_retire_queued_publications_and_unknown_sources_do_not_allocate() {
    let mut coordinator = Coordinator::default();
    coordinator
        .invalidate_source(JobId(1), SourceId(0))
        .unwrap();
    assert!(coordinator.jobs.is_empty());
    coordinator
        .enqueue(JobId(1), SourceId(0), PathBuf::from("old.par3"))
        .unwrap();
    coordinator
        .invalidate_source(JobId(1), SourceId(0))
        .unwrap();
    assert!(coordinator.jobs[&JobId(1)].pending.is_empty());
    assert_eq!(coordinator.dirty_sources(JobId(1)), [SourceId(0)]);
}

#[tokio::test]
async fn busy_job_yields_worker_capacity_to_other_jobs() {
    let root = tempfile::tempdir().unwrap();
    let path = carrier(root.path());
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), path.clone())
        .unwrap();
    coordinator
        .enqueue(JobId(1), SourceId(1), path.clone())
        .unwrap();
    coordinator.enqueue(JobId(2), SourceId(0), path).unwrap();
    for expected in [JobId(1), JobId(2), JobId(1)] {
        coordinator.dispatch().unwrap();
        let done = next(&mut coordinator).await;
        assert_eq!(coordinator.settle(done), Some(expected));
    }
}

#[tokio::test]
async fn queued_source_change_hides_the_previous_assessment() {
    let root = tempfile::tempdir().unwrap();
    let path = carrier(root.path());
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), path.clone())
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    assert_eq!(coordinator.assessments(JobId(1)).count(), 1);
    coordinator.enqueue(JobId(1), SourceId(0), path).unwrap();
    assert_eq!(coordinator.assessments(JobId(1)).count(), 0);
    coordinator.dispatch().unwrap();
    assert_eq!(coordinator.assessments(JobId(1)).count(), 0);
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    assert_eq!(coordinator.assessments(JobId(1)).count(), 1);
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
    assert!(matches!(
        coordinator.check_pending_capacity(JobId(1), WorkKey::Repair(par3_rs::InputSetId([0; 8]))),
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

fn ready_inline_repair(root: &std::path::Path) -> (Coordinator, par3_rs::InputSetId) {
    let mut runtime = Par3Job::default();
    for (id, name, bytes) in [
        (
            1,
            "a.bin",
            (0..5000u32).map(|i| (i * 7 + 3) as u8).collect::<Vec<_>>(),
        ),
        (
            2,
            "sub/c.bin",
            (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
        ),
    ] {
        let path = root.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        runtime
            .publish_file(
                SourceId(id),
                path,
                name.into(),
                std::iter::once(0..bytes.len() as u64).collect(),
            )
            .unwrap();
    }
    runtime
        .scan_file(SourceId(99), carrier(root), None)
        .unwrap();
    runtime.assess().unwrap();
    let (&set, session) = runtime.sets.first_key_value().unwrap();
    assert_eq!(
        session.view.as_ref().unwrap().status,
        par3_rs::session::RepairStatus::Ready
    );
    let mut coordinator = Coordinator::default();
    coordinator.jobs.insert(
        JobId(1),
        JobSlot {
            sources: runtime.sources.clone(),
            runtime: Some(runtime),
            ..JobSlot::default()
        },
    );
    (coordinator, set)
}

#[tokio::test]
async fn repair_results_keep_their_path_lease_through_handback_and_partial_failure() {
    for block_installation in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("output");
        std::fs::create_dir(&output).unwrap();
        if block_installation {
            std::fs::create_dir(output.join("b.txt")).unwrap();
        }
        let (mut coordinator, set) = ready_inline_repair(root.path());
        coordinator
            .request_repair(JobId(1), set, output.clone())
            .unwrap();
        let done = next(&mut coordinator).await;
        assert!(
            matches!(&done.result, Ok(WorkOutput::Repaired(result)) if result._reservation.is_some())
        );
        coordinator.settle(done);
        let completion = coordinator.take_repair_result(JobId(1)).unwrap();
        coordinator.forget(JobId(1));
        assert!(
            completion._reservation.is_some(),
            "consumer owns the lease independently of the forgotten session"
        );
        if block_installation {
            assert!(
                matches!(&completion.result, Err(EngineError::RepairInterrupted { temporary, .. }) if !temporary.is_empty())
            );
            assert!(output.join("b.txt").is_dir());
        } else {
            let report = completion.result.as_ref().unwrap();
            assert_eq!(report.installed.len(), 1);
            assert_eq!(std::fs::read(output.join("b.txt")).unwrap(), b"qrstuvwxyz");
            assert!(!output.join("a.bin").exists());
        }
    }
}

#[test]
fn discovery_hint_cannot_authorize_rewriting_a_clean_standalone_set() {
    let root = tempfile::tempdir().unwrap();
    let (mut coordinator, set) = ready_inline_repair(root.path());
    let job = coordinator.jobs.get_mut(&JobId(1)).unwrap();
    let runtime = job.runtime.as_mut().unwrap();
    let path = root.path().join("b.txt");
    std::fs::write(&path, b"qrstuvwxyz").unwrap();
    runtime
        .publish_file(
            SourceId(3),
            path,
            "b.txt".into(),
            std::iter::once(0..10).collect(),
        )
        .unwrap();
    runtime.assess().unwrap();
    assert!(runtime.sets[&set].cauchy_matrix.is_some());
    assert!(
        runtime.sets[&set]
            .view
            .as_ref()
            .unwrap()
            .requirements
            .is_empty()
    );
    job.known.insert(
        SourceId(1),
        KnownSource {
            carrier: true,
            embedded_start: Some(0),
            promoted: BTreeMap::new(),
            _reservation: assessment::ViewReservation::acquire(512).unwrap(),
        },
    );
    let (_, view) = coordinator.assessments(JobId(1)).next().unwrap();
    assert_eq!(view.status, par3_rs::session::RepairStatus::Complete);
    assert_eq!(view.embedded_source, None);
    assert!(matches!(
        coordinator.request_repair(JobId(1), set, root.path().to_owned()),
        Err(EngineError::InvalidState(_))
    ));
    assert!(coordinator.in_flight.is_empty());
    assert!(!coordinator.has_work(JobId(1)));
}

#[test]
fn excessive_repair_result_paths_are_rejected_before_dispatch_or_installation() {
    let root = tempfile::tempdir().unwrap();
    let (mut coordinator, set) = ready_inline_repair(root.path());
    let output = PathBuf::from("x".repeat(1 << 20));
    assert!(matches!(
        coordinator.request_repair(JobId(1), set, output),
        Err(EngineError::ResourceLimit(_))
    ));
    assert!(coordinator.in_flight.is_empty());
    assert!(!coordinator.has_work(JobId(1)));
    assert!(!root.path().join("b.txt").exists());
    assert_eq!(
        coordinator.assessments(JobId(1)).next().unwrap().1.status,
        par3_rs::session::RepairStatus::Ready
    );
}

fn readback_installation(path: PathBuf, options: &ExecutionOptions) -> Box<readback::Installation> {
    let output =
        readback::VerifiedOutput::capture(path, readback::STRIPE_BYTES + 1, options).unwrap();
    Box::new(readback::Installation {
        completion: RepairCompletion {
            result: Ok(Default::default()),
            outputs: Ok(vec![output]),
            _reservation: Some(assessment::ViewReservation::acquire(4096).unwrap()),
        },
        targets: vec![readback::Target {
            file: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            set: 0,
            volume: 0,
            output: 0,
            cipher: false,
            edges: Vec::new(),
        }],
        current: 0,
        offset: 0,
        crc32: 0,
        settling_set: None,
        pending_gap: None,
        edge_reads: Vec::new(),
        preflight_failed: false,
        _edge_reservation: None,
    })
}

#[tokio::test]
async fn readback_yields_between_stripes_and_fences_assessment() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("repaired.rar");
    std::fs::write(&path, vec![7; readback::STRIPE_BYTES as usize + 1]).unwrap();
    let mut coordinator = Coordinator::default();
    coordinator.admit(JobId(1)).unwrap();
    coordinator
        .queue_readback(JobId(1), readback_installation(path, &execution_options()))
        .unwrap();
    coordinator
        .enqueue(JobId(2), SourceId(1), carrier(root.path()))
        .unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    let readback = coordinator.take_readback(JobId(1)).unwrap().unwrap();
    assert!(matches!(
        readback.result.as_ref().unwrap(),
        readback::ReadbackUnit::Stripe(span) if span.len == readback::STRIPE_BYTES
    ));
    assert!(
        coordinator.has_work(JobId(1)),
        "placement still owns the completion fence"
    );
    assert_eq!(coordinator.assessments(JobId(1)).count(), 0);
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    assert_eq!(coordinator.settle(done), Some(JobId(2)));
    let readback::ReadbackDone {
        mut installation, ..
    } = readback;
    installation.offset = readback::STRIPE_BYTES;
    coordinator.queue_readback(JobId(1), installation).unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    let readback = coordinator.take_readback(JobId(1)).unwrap().unwrap();
    assert!(matches!(
        readback.result.unwrap(), readback::ReadbackUnit::Stripe(span) if span.len == 1
    ));
    coordinator.finish_installation(JobId(1));
    assert!(!coordinator.has_work(JobId(1)));
}

#[tokio::test]
async fn readback_rejects_stale_handback_without_releasing_worker_early() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("repaired.rar");
    std::fs::write(&path, vec![7; readback::STRIPE_BYTES as usize + 1]).unwrap();
    let mut coordinator = Coordinator::default();
    coordinator
        .enqueue(JobId(1), SourceId(0), carrier(root.path()))
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    coordinator
        .queue_readback(JobId(1), readback_installation(path, &execution_options()))
        .unwrap();
    let done = next(&mut coordinator).await;
    coordinator
        .invalidate_source(JobId(1), SourceId(0))
        .unwrap();
    assert_eq!(coordinator.in_flight.len(), 1);
    coordinator.settle(done);
    assert!(matches!(
        coordinator.take_readback(JobId(1)).unwrap(),
        Err(EngineError::InvalidState(_))
    ));
    assert!(coordinator.has_work(JobId(1)));
}

#[tokio::test]
async fn terminal_claims_require_current_bound_source_evidence() {
    let root = tempfile::tempdir().unwrap();
    let (mut coordinator, _) = ready_inline_repair(root.path());
    assert!(
        !coordinator.verified_file(JobId(1), SourceId(1)),
        "an unsettled set is not a terminal verdict"
    );
    let path = root.path().join("b.txt");
    std::fs::write(&path, b"qrstuvwxyz").unwrap();
    coordinator
        .enqueue_file(
            JobId(1),
            SourceId(3),
            path,
            "b.txt".into(),
            std::iter::once(0..10).collect(),
        )
        .unwrap();
    coordinator.dispatch().unwrap();
    let done = next(&mut coordinator).await;
    coordinator.settle(done);
    assert!(coordinator.verified(JobId(1)));
    for source in [SourceId(1), SourceId(2), SourceId(3)] {
        assert!(coordinator.verified_file(JobId(1), source));
    }
    assert!(
        !coordinator.verified_file(JobId(1), SourceId(99)),
        "a carrier is not a protected payload"
    );
    assert!(
        !coordinator.verified_file(JobId(1), SourceId(100)),
        "unprotected neighbours have no claim"
    );
    assert!(
        !coordinator.verified_file(JobId(2), SourceId(1)),
        "source ids are local to the job"
    );
    coordinator
        .invalidate_source(JobId(1), SourceId(3))
        .unwrap();
    assert!(
        !coordinator.verified_file(JobId(1), SourceId(3)),
        "withdrawal revokes the old claim"
    );
    assert!(
        !coordinator.verified_file(JobId(1), SourceId(1)),
        "pending reassessment hides terminal verdicts"
    );
    coordinator.forget(JobId(1));
    assert!(!coordinator.verified_file(JobId(1), SourceId(2)));
}
