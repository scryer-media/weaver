use super::*;

fn reader_pressure() -> EngineError {
    budget::source_pressure(SourceId(7), EngineError::ResourceLimit("PAR3 host state")).into()
}

fn settle_result(key: WorkKey, result: EngineResult<WorkOutput>, stale: bool) -> Coordinator {
    let mut coordinator = Coordinator::default();
    let job = JobId(1);
    coordinator.admit(job).unwrap();
    let slot = coordinator.jobs.get_mut(&job).unwrap();
    slot.ticket = Some(1);
    slot.epoch = u64::from(stale);
    let runtime = slot.runtime.take().unwrap();
    coordinator
        .in_flight
        .insert(1, (job, runtime.options.cancel.clone()));
    assert_eq!(
        coordinator.settle(WorkDone {
            job_id: job,
            ticket: 1,
            epoch: 0,
            key,
            runtime: Some(runtime),
            result,
        }),
        Some(job)
    );
    coordinator
}

#[test]
fn donor_reader_pressure_selects_the_actual_source_and_fences_dispatch() {
    let mut coordinator = settle_result(WorkKey::Donors, Err(reader_pressure()), false);
    assert!(coordinator.error(JobId(1)).is_none());
    assert!(coordinator.has_work(JobId(1)));
    coordinator.dispatch().unwrap();
    assert!(coordinator.in_flight.is_empty());
    assert_eq!(coordinator.take_spill(JobId(1)), Some(SourceId(7)));
    assert_eq!(coordinator.take_spill(JobId(1)), None);
}

#[test]
fn interrupted_repair_preserves_installations_before_spill_handback() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("verified.bin");
    std::fs::write(&path, b"verified").unwrap();
    let completion = RepairCompletion {
        result: Err(EngineError::RepairInterrupted {
            installed: vec![par3_rs::session_repair::InstalledFile {
                path: path.clone(),
                backup: None,
            }],
            temporary: vec![root.path().join(".par3-repair-1-1.tmp")],
            cause: Box::new(reader_pressure()),
        }),
        outputs: Ok(Vec::new()),
        embedded_replacement: false,
        _reservation: None,
    };
    let mut coordinator = settle_result(
        WorkKey::Repair(par3_rs::InputSetId([0; 8])),
        Ok(WorkOutput::Repaired(completion)),
        false,
    );
    assert_eq!(
        coordinator.take_spill(JobId(1)),
        None,
        "repair consumer must reconcile first"
    );
    let completion = coordinator.take_repair_result(JobId(1)).unwrap();
    assert!(
        matches!(completion.result, Err(EngineError::RepairInterrupted { installed, temporary, .. })
            if temporary.len() == 1 && installed.len() == 1 && installed[0].path == path)
    );
    assert_eq!(std::fs::read(path).unwrap(), b"verified");
    assert_eq!(coordinator.take_spill(JobId(1)), Some(SourceId(7)));
}

#[test]
fn non_pressure_and_stale_outcomes_do_not_select_spill() {
    for error in [
        EngineError::Cancelled,
        EngineError::SourceChanged(SourceId(7)),
        EngineError::Io(std::io::Error::other("disk failed")),
        EngineError::InvalidState("invalid layout"),
        EngineError::ResourceLimit("native memory"),
    ] {
        let mut coordinator = settle_result(WorkKey::Donors, Err(error), false);
        assert!(coordinator.error(JobId(1)).is_some());
        assert_eq!(coordinator.take_spill(JobId(1)), None);
    }
    let mut coordinator = settle_result(WorkKey::Donors, Err(reader_pressure()), true);
    assert_eq!(coordinator.take_spill(JobId(1)), None);
}
