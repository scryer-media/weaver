use super::*;

fn empty_placement_plan() -> par2_rs::PlacementPlan {
    par2_rs::PlacementPlan {
        exact: vec![],
        swaps: vec![],
        renames: vec![],
        unresolved: vec![],
        conflicts: vec![],
    }
}

#[tokio::test]
async fn exhausted_promoted_rar_recovery_reaches_the_next_wave() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(30803);
    let volumes = build_multifile_multivolume_rar_set();
    let described: Vec<_> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let index = build_test_par2_index_for_files(&described, 64);
    let full_set = build_repairable_par2_set_for_files(&described, 64, 4);
    let recovered_slices: Vec<_> = (0..4)
        .map(|i| (i, full_set.recovery_slices[&i].data.as_bytes().unwrap()))
        .collect();
    let recovery =
        build_test_par2_recovery_volume(*full_set.recovery_set_id.as_bytes(), &recovered_slices);
    let mut files = volumes.clone();
    files.push(("show.par2".into(), index.clone()));
    files.push(("show.vol00+01.par2".into(), vec![0; 64]));
    files.push(("show.vol01+04.par2".into(), recovery.clone()));
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Exhausted RAR recovery", &files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    for (i, (name, bytes)) in volumes.iter().enumerate() {
        if i != 1 && i != 3 {
            write_and_complete_rar_volume(&mut pipeline, job_id, i as u32, name, bytes).await;
        }
    }
    write_and_complete_file(&mut pipeline, job_id, 4, "show.par2", &index).await;
    let mut installed_set = full_set;
    installed_set.recovery_slices.clear();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        installed_set,
        &[
            (4, "show.par2", 0, false),
            (5, "show.vol00+01.par2", 1, true),
            (6, "show.vol01+04.par2", 4, false),
        ],
    );
    let lost = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 5,
        },
        segment_number: 0,
    };
    pipeline.mark_promoted_recovery_segment_unavailable(lost);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 6,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("recovery-next@example.com"),
            groups: Arc::from(vec!["alt.binaries.test".into()]),
            priority: 1000,
            byte_estimate: recovery.len() as u32,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: vec![],
            avoid_server: None,
        });
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);
    assert!(pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id));
    for _ in 0..3 {
        pipeline.check_job_completion(job_id).await;
        settle_par2_analysis_work(&mut pipeline).await;
    }
    assert!(
        pipeline.is_promoted_recovery_file(job_id, 6),
        "an exhausted incomplete recovery file must not hold settlement forever"
    );
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    write_and_complete_file(&mut pipeline, job_id, 6, "show.vol01+04.par2", &recovery).await;
    pipeline
        .try_load_par2_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 6,
            },
        )
        .await;
    drain_job_to_completion(&mut pipeline, job_id).await;
    assert!(pipeline.par2_verified.contains(&job_id));
    // The direct harness must service the topology refresh messages that
    // the running orchestrator normally handles before extraction starts.
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 8).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Exhausted RAR recovery",
    ));
    assert_eq!(
        std::fs::read(destination.join("E01.mkv")).unwrap(),
        b"episode-a-payload"
    );
    assert_eq!(
        std::fs::read(destination.join("E02.mkv")).unwrap(),
        b"episode-b-payload"
    );
}

#[tokio::test]
async fn placement_closed_cycles_install_bytes_before_rebinding_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(30801);
    // Two disjoint permutations, with the lengths seen in a stalled placement
    // report. Every destination exists; no independent rename can start.
    let destinations = [
        8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 0, 18, 19, 20, 1, 2, 3, 4, 5, 6, 7,
    ];
    let names: Vec<_> = (0..21).map(|i| format!("part{:02}.bin", i + 1)).collect();
    let spec = standalone_job_spec(
        "Closed placement cycles",
        &names.iter().map(|n| (n.clone(), 32)).collect::<Vec<_>>(),
    );
    let dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let mut plan = empty_placement_plan();
    for (source, &destination) in destinations.iter().enumerate() {
        write_and_complete_file(
            &mut pipeline,
            job_id,
            source as u32,
            &names[source],
            &[source as u8; 32],
        )
        .await;
        plan.renames.push(par2_rs::PlacementEntry {
            file_id: par2_rs::FileId::from_bytes([source as u8; 16]),
            current_name: names[source].clone(),
            correct_name: names[destination].clone(),
        });
    }
    pipeline
        .apply_placement_plan_for_retry_or_repair(job_id, dir.clone(), &plan)
        .await
        .unwrap();
    for (source, &destination) in destinations.iter().enumerate() {
        assert_eq!(
            std::fs::read(dir.join(&names[destination])).unwrap(),
            vec![source as u8; 32],
            "destination {} must contain its actual owner",
            names[destination]
        );
        let identity = pipeline
            .file_identity(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: source as u32,
                },
            )
            .unwrap();
        assert_eq!(identity.current_filename, names[destination]);
    }
    assert!(
        std::fs::read_dir(&dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".weaver-placement-")),
        "no staging files remain"
    );
}

#[tokio::test]
async fn placement_journal_ignores_unchanged_identity_alongside_moves() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(30803);
    let dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Mixed placement",
            &[("a.bin".into(), 32), ("b.bin".into(), 32)],
        ),
    )
    .await;
    let mut plan = empty_placement_plan();
    for (index, source, destination) in [(0, "a.bin", "new.bin"), (1, "b.bin", "b.bin")] {
        write_and_complete_file(&mut pipeline, job_id, index, source, &[index as u8; 32]).await;
        plan.renames.push(par2_rs::PlacementEntry {
            file_id: par2_rs::FileId::from_bytes([index as u8; 16]),
            current_name: source.into(),
            correct_name: destination.into(),
        });
    }
    pipeline
        .apply_placement_plan_for_retry_or_repair(job_id, dir.clone(), &plan)
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.join("new.bin")).unwrap(), [0; 32]);
    assert_eq!(std::fs::read(dir.join("b.bin")).unwrap(), [1; 32]);
    for (file_index, filename) in [(0, "new.bin"), (1, "b.bin")] {
        assert_eq!(
            pipeline
                .effective_file_identity(job_id, NzbFileId { job_id, file_index })
                .unwrap()
                .current_filename,
            filename
        );
    }
    assert!(placement::recover(&dir).unwrap().transactions.is_empty());
}

#[tokio::test]
async fn placement_occupied_unrelated_destination_does_not_rebind_or_partially_move() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(30802);
    let dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Placement collision",
            &[("a.bin".into(), 32), ("b.bin".into(), 32)],
        ),
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 0, "a.bin", &[1; 32]).await;
    write_and_complete_file(&mut pipeline, job_id, 1, "b.bin", &[2; 32]).await;
    std::fs::write(dir.join("occupied.bin"), [3; 32]).unwrap();
    let mut plan = empty_placement_plan();
    for (index, source, destination) in [(0, "a.bin", "free.bin"), (1, "b.bin", "occupied.bin")] {
        plan.renames.push(par2_rs::PlacementEntry {
            file_id: par2_rs::FileId::from_bytes([index; 16]),
            current_name: source.into(),
            correct_name: destination.into(),
        });
    }
    assert!(
        pipeline
            .apply_placement_plan_for_retry_or_repair(job_id, dir.clone(), &plan)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(dir.join("a.bin")).unwrap(), vec![1; 32]);
    assert_eq!(std::fs::read(dir.join("b.bin")).unwrap(), vec![2; 32]);
    assert_eq!(
        std::fs::read(dir.join("occupied.bin")).unwrap(),
        vec![3; 32]
    );
    assert!(!dir.join("free.bin").exists());
    assert!(
        pipeline
            .file_identity(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: 0
                }
            )
            .is_none(),
        "a failed plan must not invent a canonical identity"
    );
}

// ---------------------------------------------------------------------------
// A parked repair verdict against an extraction phase.
//
// A job whose sets extract independently enters `Extracting` for whichever set
// is ready first and stays there after that set's members are out, while a
// sibling set still waits on its repair. The fixtures below hold one damaged
// set with a parked verdict and put the job in that idle extraction phase
// before its recovery lands.
// ---------------------------------------------------------------------------

// Parks the verdict, then leaves the job in `status` with no extraction work
// in flight: the state a sibling set's finished extraction leaves behind.
async fn park_verdict_then_idle_extraction_phase(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    status: JobStatus,
) -> ParkedRecoveryFixture {
    let fixture = install_parked_recovery_par2_job(pipeline, job_id, job_name).await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(pipeline).await;
    assert_eq!(
        job_status_for_assert(pipeline, job_id),
        Some(JobStatus::Downloading),
        "the analysis promoted the volume and parked on it; {}",
        debug_job_state(pipeline, job_id)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);

    let persist = match status {
        JobStatus::Extracting => "extracting",
        JobStatus::QueuedExtract => "queued_extract",
        ref other => panic!("not an extraction phase: {other:?}"),
    };
    pipeline.transition_postprocessing_status(job_id, status.clone(), Some(persist));
    assert_eq!(job_status_for_assert(pipeline, job_id), Some(status));
    assert!(!pipeline.job_has_active_extraction_tasks(job_id));
    fixture
}

async fn assert_landed_recovery_repairs_out_of_idle_extraction_phase(
    job_id: JobId,
    job_name: &str,
    status: JobStatus,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let fixture =
        park_verdict_then_idle_extraction_phase(&mut pipeline, job_id, job_name, status).await;

    land_parked_recovery_volume(&mut pipeline, job_id, &fixture).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        pipeline.par2_repairs_from_parked_verdict,
        1,
        "the landed recovery repaired on the parked verdict; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
    let mut repair_started = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, PipelineEvent::RepairStarted { job_id: started } if started == job_id) {
            repair_started = true;
        }
    }
    assert!(repair_started, "the repair phase was entered");
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "the repaired job settled; {}",
        debug_job_state(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_landed_recovery_repairs_while_a_finished_sibling_extraction_holds_the_phase() {
    assert_landed_recovery_repairs_out_of_idle_extraction_phase(
        JobId(30960),
        "Silver Horizon Idle Extracting Repair",
        JobStatus::Extracting,
    )
    .await;
}

#[tokio::test]
async fn a_landed_recovery_repairs_while_the_job_is_queued_for_extraction() {
    assert_landed_recovery_repairs_out_of_idle_extraction_phase(
        JobId(30961),
        "Silver Horizon Queued Extract Repair",
        JobStatus::QueuedExtract,
    )
    .await;
}

// A live extraction still owns the job: the repair waits for its settlement,
// and the same verdict repairs once the extraction is idle.
#[tokio::test]
async fn a_landed_recovery_waits_for_a_live_extraction_then_repairs_on_the_same_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30962);
    let fixture = park_verdict_then_idle_extraction_phase(
        &mut pipeline,
        job_id,
        "Silver Horizon Live Extraction Repair",
        JobStatus::Extracting,
    )
    .await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("sibling".to_string());

    land_parked_recovery_volume(&mut pipeline, job_id, &fixture).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(pipeline.par2_repairs_from_parked_verdict, 0);
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Extracting),
        "the live extraction keeps the phase; {}",
        debug_job_state(&pipeline, job_id)
    );

    pipeline.inflight_extractions.remove(&job_id);
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        pipeline.par2_repairs_from_parked_verdict,
        1,
        "the settled extraction released the parked verdict; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_bytes_read.len(),
        1,
        "the wait did not cost a second analysis"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
}

#[tokio::test]
async fn the_repair_phase_takes_an_idle_extraction_phase_and_refuses_a_live_one() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30963);
    install_parked_recovery_par2_job(&mut pipeline, job_id, "Silver Horizon Repair Phase Gate")
        .await;

    // A RAR worker still running.
    pipeline.transition_postprocessing_status(job_id, JobStatus::Extracting, Some("extracting"));
    pipeline.rar_sets.insert(
        (job_id, "sibling".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            active_workers: 1,
            ..Default::default()
        },
    );
    assert!(!pipeline.maybe_start_repair(job_id).await);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Extracting)
    );

    // A batch still in flight.
    pipeline
        .rar_sets
        .get_mut(&(job_id, "sibling".to_string()))
        .unwrap()
        .active_workers = 0;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("sibling".to_string());
    assert!(!pipeline.maybe_start_repair(job_id).await);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Extracting)
    );

    // Idle.
    pipeline.inflight_extractions.remove(&job_id);
    assert!(pipeline.maybe_start_repair(job_id).await);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Repairing)
    );
}
