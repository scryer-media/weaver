//! `rar_extraction` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn no_par2_retry_reclassifies_obfuscated_rar_redownload_as_7z() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let filename = "51273aad56a8b904e96928935278a627";
    let rar_bytes = rar5_fixture_bytes("rar5_store.rar");
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = rar_job_spec(
        "Obfuscated RAR Retry Reclassifies As 7z",
        &[(filename.to_string(), rar_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &rar_bytes).await;
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([filename.to_string()]));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap();
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            FileRole::Unknown
        ));
        assert!(state.assembly.archive_topology_for(filename).is_none());
    }
    assert!(pipeline.rar_sets.is_empty());
    assert!(
        pipeline
            .db
            .load_all_archive_headers(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .is_empty()
    );

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &seven_zip_bytes).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let file = state
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    assert!(matches!(
        pipeline.classified_role_for_file(job_id, file),
        weaver_model::files::FileRole::SevenZipArchive
    ));
    let topology = state
        .assembly
        .archive_topology_for(filename)
        .expect("replacement payload should create a fresh 7z topology");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert!(topology.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness(filename),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn re_registering_identical_rar_facts_still_recomputes_readiness() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30030);
    let filename = "archive.rar";
    let fixture_bytes = rar5_fixture_bytes("rar5_store.rar");
    let spec = rar_job_spec(
        "RAR Recompute On Same Facts",
        &[(filename.to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let topo = state.assembly.archive_topology_for_mut("archive").unwrap();
        topo.complete_volumes.clear();
    }

    pipeline
        .refresh_archive_state_for_completed_file(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            true,
        )
        .await;
    drain_rar_refreshes(&mut pipeline).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_rar_failure_requeues_member_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30027);
    let files = vec![("archive.rar".to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "work/sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["work/sample.mkv".to_string()]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn no_par2_runtime_rar_failure_requeues_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let filename = "51273aad56a8b904e96928935278a627";
    let set_name = filename.to_string();
    let member_name = "sample.mkv".to_string();
    let files = vec![(filename.to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 Runtime RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([(filename.to_string(), 0)]),
        complete_volumes: [0u32].into_iter().collect(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            volume_files: std::collections::BTreeMap::from([(0, filename.to_string())]),
            phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec![member_name.clone()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([member_name]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.rar_sets.contains_key(&(job_id, set_name)));
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn exhausted_rar_failed_member_skips_lower_bound_recovery_preflight() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30191);
    let filename = "archive.rar";
    let files = vec![(filename.to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("Exhausted RAR Lower Bound Skip", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.assembly.set_archive_topology(
            "archive".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([(filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    pipeline
        .test_promote_recovery_for_failed_member(job_id, "archive", "sample.mkv")
        .await;

    assert_eq!(pipeline.par2_lower_bound_preflight_calls, 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 0);
}

#[tokio::test]
async fn verified_par2_output_registers_missing_rar_volume_for_retry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30191);
    let files = build_multifile_multivolume_rar_set();
    let present_files = vec![files[0].clone(), files[1].clone(), files[3].clone()];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Missing Volume PAR2 Registration", &present_files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in present_files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let (missing_filename, missing_bytes) = &files[2];
    tokio::fs::write(working_dir.join(missing_filename), missing_bytes)
        .await
        .unwrap();
    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_rs::FileId::from_bytes([0; 16]),
            filename: missing_filename.clone(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: Vec::new(),
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    assert_eq!(
        pipeline
            .register_verified_par2_rar_outputs(job_id, &verification)
            .await
            .unwrap()
            .registered,
        1
    );
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let rar_set = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("RAR set should be registered");
    assert!(rar_set.facts.contains_key(&2));
    assert!(
        rar_set
            .plan
            .as_ref()
            .is_some_and(|plan| plan.waiting_on_volumes.is_empty())
    );
}

#[tokio::test]
async fn reconciled_obfuscated_rar_keeps_the_verified_canonical_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30195);
    let files = build_multifile_multivolume_rar_set();
    let source_filename = "ae282dbe64861b7171e041b55057e3dd.40";
    let canonical_filename = files[1].0.as_str();
    let duplicate_filename = "show.part02.duplicate1.rar";
    let posted_files = vec![
        files[0].clone(),
        (source_filename.to_string(), files[1].1.clone()),
        files[2].clone(),
        files[3].clone(),
    ];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Obfuscated RAR Repair Reconciliation", &posted_files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for file_index in [0usize, 2, 3] {
        write_and_complete_rar_volume(
            &mut pipeline,
            job_id,
            file_index as u32,
            &posted_files[file_index].0,
            &posted_files[file_index].1,
        )
        .await;
    }
    tokio::fs::write(working_dir.join(canonical_filename), &files[1].1)
        .await
        .unwrap();
    let mut damaged_duplicate = files[1].1.clone();
    let payload_offset = damaged_duplicate
        .windows(b"a-payload".len())
        .position(|window| window == b"a-payload")
        .unwrap();
    damaged_duplicate[payload_offset] ^= 1;
    tokio::fs::write(working_dir.join(duplicate_filename), damaged_duplicate)
        .await
        .unwrap();

    let file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: file_id.file_index,
                source_filename: source_filename.to_string(),
                current_filename: duplicate_filename.to_string(),
                canonical_filename: Some(duplicate_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(1),
                }),
                classification_source: FileIdentitySource::Par2,
            },
        )
        .unwrap();
    pipeline.file_prefix_16k.insert(
        file_id,
        files[1].1[..files[1].1.len().min(16 * 1024)].to_vec(),
    );

    let par2_set = placement_par2_file_set(&files);
    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_set.recovery_file_ids[1],
            filename: canonical_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    assert_eq!(
        pipeline
            .register_verified_par2_rar_outputs(job_id, &verification)
            .await
            .unwrap()
            .registered,
        1
    );
    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();
    assert_eq!(report.completed, 1);
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let identity = pipeline.file_identity(job_id, file_id).unwrap();
    assert_eq!(identity.source_filename, source_filename);
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(
        volume_paths.get(&1),
        Some(&working_dir.join(canonical_filename))
    );

    let mut archive =
        unrar_rs::RarArchive::open(std::fs::File::open(volume_paths.get(&0).unwrap()).unwrap())
            .unwrap();
    for volume in 1..files.len() {
        archive
            .add_volume(
                volume,
                Box::new(std::fs::File::open(volume_paths.get(&(volume as u32)).unwrap()).unwrap()),
            )
            .unwrap();
    }
    let output_dir = working_dir.join("regression-output");
    std::fs::create_dir_all(&output_dir).unwrap();
    let member_index = archive.find_member_sanitized("E01.mkv").unwrap();
    Pipeline::extract_rar_member_to_output(
        &mut archive,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            job_id,
            "show",
            &output_dir,
            &unrar_rs::ExtractOptions {
                verify: true,
                password: None,
                restore_owners: false,
            },
        ),
        member_index,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(output_dir.join("E01.mkv")).unwrap(),
        b"episode-a-payload"
    );
}

#[tokio::test]
async fn failed_rar_member_with_par2_is_not_incrementally_retried() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30192);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR PAR2 Retry Latch", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    let set_key = (job_id, "show".to_string());
    let failed_member = "E01.mkv".to_string();
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("RAR set should exist after all volumes complete");
        let plan = set_state
            .plan
            .as_mut()
            .expect("RAR set should have an extraction plan");
        plan.ready_members = vec![crate::pipeline::rar_state::RarReadyMember {
            name: failed_member.clone(),
        }];
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([failed_member]));
    pipeline.par2_verified.insert(job_id);

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("RAR set should remain available");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());
}

#[tokio::test]
async fn missing_middle_rar_volume_enters_authoritative_par2_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30193);
    let missing_filename = "archive.part3.rar";
    let index_filename = "archive.par2";
    let recovery_filename = "archive.vol00+01.par2";
    let missing_bytes: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let index_bytes = build_test_par2_index(missing_filename, &missing_bytes, 64);
    let files = vec![
        ("archive.part1.rar".to_string(), vec![0x11; 64]),
        ("archive.part2.rar".to_string(), vec![0x22; 64]),
        (missing_filename.to_string(), missing_bytes.clone()),
        ("archive.part4.rar".to_string(), vec![0x44; 64]),
        (index_filename.to_string(), index_bytes.clone()),
        (recovery_filename.to_string(), vec![0xAA; 64]),
    ];
    let spec = rar_job_spec("RAR Missing Middle PAR2 Recovery", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for file_index in [0u32, 1, 3] {
        let (filename, bytes) = &files[file_index as usize];
        write_and_complete_file(&mut pipeline, job_id, file_index, filename, bytes).await;
    }
    write_and_complete_file(&mut pipeline, job_id, 4, index_filename, &index_bytes).await;

    let archive_topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("archive.part1.rar".to_string(), 0),
            ("archive.part2.rar".to_string(), 1),
            ("archive.part4.rar".to_string(), 3),
        ]),
        complete_volumes: [0u32, 1, 3].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "movie.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 2,
        }],
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 5,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("missing-middle-recovery@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            // Optional PAR2 volumes enter the parked recovery queue before the
            // authoritative analyzer classifies and promotes them.
            is_recovery: false,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .set_archive_topology("archive".to_string(), archive_topology.clone());
    }
    pipeline.rar_sets.insert(
        (job_id, "archive".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: true,
                ready_members: Vec::new(),
                member_names: vec!["movie.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: archive_topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(missing_filename, &missing_bytes, 64, 0),
        &[
            (4, index_filename, 0, false),
            (5, recovery_filename, 2, false),
        ],
    );
    assert!(pipeline.par2_set(job_id).is_some());
    assert!(!pipeline.par2_bypassed.contains(&job_id));
    pipeline.par2_verified.insert(job_id);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert!(!pipeline.job_has_active_extraction_tasks(job_id));
    assert!(pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| !state.recovery_queue.is_empty())
    );

    while events.try_recv().is_ok() {}
    pipeline.par2_repairer_analyze_calls = 0;
    pipeline.check_job_completion(job_id).await;

    let status = pipeline.jobs.get(&job_id).map(|state| state.status.clone());
    assert_eq!(
        pipeline.par2_repairer_analyze_calls,
        1,
        "missing RAR volume must force authoritative PAR2 verification; status={status:?}, failed={:?}, verified={}",
        pipeline.failed_extractions.get(&job_id),
        pipeline.par2_verified.contains(&job_id),
    );
    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
    assert_eq!(status, Some(JobStatus::Downloading));
    assert!(pipeline.jobs.get(&job_id).is_some_and(|state| {
        state
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 5)
            > 0
    }));
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        2
    );

    // Optional PAR2 files preserve the NZB's original classification, so they
    // are not necessarily marked `is_recovery`. A completion check triggered
    // by one recovered file must wait for every promoted file, not rescan.
    pipeline.check_job_completion(job_id).await;
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
}

#[tokio::test]
async fn rar_waiting_for_missing_volumes_without_par2_fails_after_download_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30077);
    let working_dir = intermediate_dir.join("rar-missing-tail");
    let mut state = minimal_job_state(job_id, "RAR Missing Tail", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("no PAR2 metadata is available for repair"));
}

#[tokio::test]
async fn legacy_reconcile_schedules_waiting_rar_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    let working_dir = intermediate_dir.join("rar-waiting-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting No Spin", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.reconcile_job_progress(job_id).await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn rar_completion_waiting_for_volumes_does_not_requeue_itself() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let working_dir = intermediate_dir.join("rar-waiting-check-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting Check No Spin", working_dir);
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn clean_member_keeps_failed_neighbor_boundary_volume_suspect() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Boundary Suspect Claims", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
                (2u32, dummy_rar_volume_facts(2)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::from([1u32]),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    let suspect = pipeline.suspect_rar_volumes_for_job(job_id);
    let decision = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .and_then(|plan| plan.delete_decisions.get(&1))
        .unwrap();

    assert!(suspect.contains(&1));
    assert!(!Pipeline::claim_clean_rar_volume(decision));
}

#[tokio::test]
async fn normalization_refresh_preserves_deleted_untouched_rar_facts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30016);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalization Keeps Facts", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string(), "show.part04.rar".to_string()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

    let facts: Vec<u32> = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("RAR set should exist")
        .facts
        .keys()
        .copied()
        .collect();
    assert_eq!(facts, vec![0, 1, 2, 3]);
    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .map(|rows| rows.len()),
        Some(4)
    );
}

#[tokio::test]
async fn clean_verify_after_file_swap_refreshes_stale_rar_snapshot_without_volume_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30021);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Refreshes Snapshot", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

    for (file_index, (filename, _)) in files.iter().enumerate() {
        let current_bytes = tokio::fs::read(working_dir.join(filename)).await.unwrap();
        persist_completed_file_hash(
            &pipeline,
            job_id,
            file_index as u32,
            filename,
            &current_bytes,
        )
        .await;
    }

    tokio::fs::remove_file(working_dir.join(&files[0].0))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join(&files[1].0))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        [files[0].0.clone(), files[1].0.clone()]
            .into_iter()
            .collect(),
    );

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after placement correction");
    let suspect_volumes = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .expect("RAR state should exist after placement correction");
    let part03_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .cloned()
        .expect("swapped volume identity should persist after placement correction");
    let part04_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .cloned()
        .expect("counterpart swapped volume identity should persist after placement correction");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert_eq!(suspect_volumes, [2, 3].into_iter().collect());
    assert_eq!(part03_identity.current_filename, "show.part04.rar");
    assert_eq!(
        part03_identity.canonical_filename.as_deref(),
        Some("show.part04.rar")
    );
    assert_eq!(
        part03_identity.classification_source,
        FileIdentitySource::Par2
    );
    assert_eq!(part04_identity.current_filename, "show.part03.rar");
    assert_eq!(
        part04_identity.canonical_filename.as_deref(),
        Some("show.part03.rar")
    );
    assert_eq!(
        part04_identity.classification_source,
        FileIdentitySource::Par2
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part03.rar")
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
        Some("show.part04.rar")
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv"),
        "stale cached headers must be refreshed so the swapped member can retry: {:?}",
        plan.waiting_on_volumes
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental retry batch"),
    }
}

#[tokio::test]
async fn ownerless_live_rar_plan_error_requires_named_member_facts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30083);
    let working_dir = temp_dir.path().join("ownerless-live-guard");
    let mut state = minimal_job_state(job_id, "Ownerless Live Guard", working_dir);
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([("show.part01.rar".to_string(), 0)]),
        complete_volumes: [0u32].into_iter().collect(),
        expected_volume_count: Some(1),
        members: Vec::new(),
        unresolved_spans: Vec::new(),
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    let mut stale_named_facts = dummy_named_rar_volume_facts(0, "E01.mkv");
    stale_named_facts.members[0].is_directory = true;

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: BTreeMap::from([(0u32, stale_named_facts)]),
            volume_files: BTreeMap::from([(0u32, "show.part01.rar".to_string())]),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: BTreeMap::from([(
                    0u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: Vec::new(),
                        clean_owners: Vec::new(),
                        failed_owners: Vec::new(),
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    let error = pipeline
        .ownerless_live_rar_plan_error_for_job(job_id)
        .expect("named member facts should make ownerless live plans invalid");
    assert!(error.contains("ownerless present RAR volumes"));

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .facts
        .insert(0, dummy_rar_volume_facts(0));
    assert!(
        pipeline
            .ownerless_live_rar_plan_error_for_job(job_id)
            .is_none(),
        "empty malformed facts stay non-fatal for restore-time eager-delete auditing"
    );
}

#[tokio::test]
async fn rar_completion_waits_for_pending_refresh_before_terminal_decision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30084);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Pending Refresh Barrier", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }

    pipeline.rar_refresh_state.insert(
        (job_id, "show".to_string()),
        RarRefreshState {
            in_flight: Some(RarRefreshRequest {
                target_completed_volume: 3,
                reason: RefreshReason::CoverageExpansion,
            }),
            queued: None,
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.job_has_pending_rar_refresh_for_current_sets(job_id));
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(
        !matches!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Failed { .. })
        ),
        "completion should not fail or repair-evaluate while a current RAR refresh is pending"
    );
    assert!(
        pipeline.job_has_incoherent_rar_waiting_state(job_id),
        "pending-refresh barrier should not force an eager topology rebuild"
    );
}

#[tokio::test]
async fn incoherent_rar_waiting_state_heals_before_reverification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30080);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incoherent Waiting Heals Before Verify", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let mut good_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    cached["members"] = serde_json::json!([]);
    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.cached_headers = Some(stale_headers.clone());
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(!pipeline.job_has_incoherent_rar_waiting_state(job_id));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after healing");
    assert!(
        !matches!(
            plan.phase,
            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
        ),
        "completion healing should not leave the set in an impossible waiting state",
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E01.mkv"),
        "healed plan should restore ready members without another PAR2 verification",
    );
}

#[tokio::test]
async fn retry_archive_extraction_after_verify_or_repair_heals_incoherent_rar_state_for_mixed_archive_jobs()
 {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30081);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Mixed Archive Heal", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            "bonus".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Split,
                volume_map: std::collections::HashMap::from([("bonus.001".to_string(), 0u32)]),
                complete_volumes: std::collections::HashSet::from([0u32]),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "bonus.bin".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 10,
                }],
                unresolved_spans: Vec::new(),
            },
        );

    let mut good_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    cached["members"] = serde_json::json!([]);
    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.cached_headers = Some(stale_headers.clone());
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();
    pipeline.inflight_extractions.insert(
        job_id,
        ["show".to_string(), "bonus".to_string()]
            .into_iter()
            .collect(),
    );

    pipeline
        .retry_archive_extraction_after_verify_or_repair(job_id)
        .await;

    assert!(!pipeline.job_has_incoherent_rar_waiting_state(job_id));
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after mixed-archive healing");
    assert!(
        !matches!(
            plan.phase,
            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
        ),
        "mixed-archive retry should heal the impossible waiting state",
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E01.mkv"),
        "mixed-archive retry should restore live member readiness",
    );
}

#[tokio::test]
async fn retry_archive_extraction_after_verify_or_repair_preserves_recoverable_rar_fallback_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30082);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Recoverable Fallback", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();

    let corrupt_headers = vec![0xFF, 0x00, 0x13];
    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after all volumes complete")
        .cached_headers = Some(corrupt_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &corrupt_headers)
        .unwrap();

    pipeline
        .retry_archive_extraction_after_verify_or_repair(job_id)
        .await;

    assert!(!matches!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Failed { .. })
    ));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("fallback plan should be retained after recompute failure");
    assert!(matches!(
        plan.phase,
        crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
    ));
    assert!(plan.fallback_reason.as_deref().is_some_and(|reason| {
        reason.contains("cannot be rebuilt without cached headers or volume 0")
    }));
}

#[tokio::test]
async fn rar_retry_frontier_rejects_waiting_on_deleted_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30018);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Retry Frontier Rejects Deleted Waiting Volume", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part04.rar"))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part04.rar".to_string()].into_iter().collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert!(plan.waiting_on_volumes.contains(&3));
    assert!(!plan.deletion_eligible.contains(&3));
    assert_eq!(
        pipeline.invalid_rar_retry_frontier_reason(job_id),
        Some("set 'show' waiting volumes already deleted: [3]".to_string())
    );
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_some());
}

#[tokio::test]
async fn eager_delete_retains_volume_with_failed_member_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30019);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Failed Claim Delete Guard", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (filename, bytes) in &files {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline
        .failed_extractions
        .insert(job_id, ["E10.mkv".to_string()].into_iter().collect());
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(1u32, dummy_rar_volume_facts(1))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: [1u32].into_iter().collect(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part02.rar").exists());
    assert!(
        !pipeline
            .eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains("show.part02.rar"))
    );
}

#[tokio::test]
async fn paused_queued_extraction_is_not_promoted_when_capacity_frees() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30089);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Paused Queued Extract", &[("archive.7z".to_string(), 100)]),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Paused;
        state.paused_resume_status = Some(JobStatus::Downloading);
        state.queued_extract_at_epoch_ms = Some(crate::jobs::model::epoch_ms_now());
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.promote_queued_extractions();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Paused));
    assert!(matches!(
        state.paused_resume_status,
        Some(JobStatus::Downloading)
    ));
}

#[tokio::test]
async fn queued_repair_blocks_extraction_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Awaiting Repair Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::QueuedRepair;
        state.refresh_runtime_lanes_from_status();
    }

    assert!(!pipeline.maybe_start_extraction(job_id).await);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::QueuedRepair));
}

#[tokio::test]
async fn exhausted_incomplete_rar_member_is_not_scheduled_for_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Incomplete RAR Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    let set_name = "archive".to_string();
    let member_name = "work/sample.mkv".to_string();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
        complete_volumes: HashSet::new(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 100,
        }],
        unresolved_spans: Vec::new(),
    };

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state
            .assembly
            .set_archive_topology(set_name.clone(), topology.clone());
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: vec![crate::pipeline::archive::rar_state::RarReadyMember {
                    name: member_name,
                }],
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    pipeline.try_rar_extraction(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!matches!(state.status, JobStatus::Extracting));
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, set_name))
            .map(|set| set.active_workers),
        Some(0)
    );
}

#[tokio::test]
async fn incomplete_download_with_active_extraction_defers_instead_of_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let spec = JobSpec {
        name: "Incomplete Download Active Extraction".to_string(),
        password: None,
        total_bytes: 320,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 128,
                        message_id: "active-extract-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 128,
                        message_id: "active-extract-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            message_id: MessageId::new("active-extract-payload-1@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 0,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(1, index_filename, 0, false)],
    );
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("payload".to_string());

    pipeline.check_job_completion(job_id).await;

    let status = job_status_for_assert(&pipeline, job_id);
    assert!(!matches!(status, Some(JobStatus::Failed { .. })));
    assert!(pipeline.jobs.contains_key(&job_id));
}

#[tokio::test]
async fn rar_unlock_non_solid_boosts_next_member_volume_then_recomputes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40100);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Non Solid", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[2, 3, 4]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![
            crate::jobs::assembly::ArchiveMember {
                name: "done.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 10,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "next-small.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 20,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "later-wide.mkv".to_string(),
                first_volume: 3,
                last_volume: 4,
                unpacked_size: 30,
            },
        ],
    );
    pipeline
        .extracted_members
        .insert(job_id, HashSet::from(["done.mkv".to_string()]));

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let first = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(first.segment_id.file_id.file_index, 2);
    assert_eq!(first.priority, 3);

    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert("next-small.mkv".to_string());
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3, 4]);
    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let next = state.download_queue.pop().unwrap();
    let after_next = state.download_queue.pop().unwrap();
    assert_eq!(next.segment_id.file_id.file_index, 3);
    assert_eq!(next.priority, 3);
    assert_eq!(after_next.segment_id.file_id.file_index, 4);
    assert_eq!(after_next.priority, 3);
}

#[tokio::test]
async fn rar_unlock_solid_boosts_only_earliest_sequential_missing_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40101);
    let files = (0..7)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Solid", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[1, 2, 3, 4, 5, 6]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        true,
        7,
        HashSet::from([0]),
        vec![
            crate::jobs::assembly::ArchiveMember {
                name: "solid-first.mkv".to_string(),
                first_volume: 0,
                last_volume: 5,
                unpacked_size: 100,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "solid-later.mkv".to_string(),
                first_volume: 6,
                last_volume: 6,
                unpacked_size: 1,
            },
        ],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let boosted = (0..4)
        .map(|_| state.download_queue.pop().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        boosted
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(boosted.iter().all(|work| work.priority == 3));
    let unboosted = state.download_queue.pop().unwrap();
    assert_eq!(unboosted.segment_id.file_id.file_index, 5);
    assert_eq!(unboosted.priority, 15);
}

#[tokio::test]
async fn rar_unlock_uses_par2_authoritative_identity_for_obfuscated_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40102);
    let spec = JobSpec {
        name: "RAR Unlock PAR2 Identity".to_string(),
        password: None,
        total_bytes: 8,
        category: None,
        metadata: Vec::new(),
        files: vec![FileSpec {
            filename: "obfuscated.bin".to_string(),
            role: FileRole::Unknown,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 8,
                message_id: "rar-unlock-obfuscated@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(rar_unlock_work(
            job_id,
            0,
            FileRole::Unknown.download_priority(),
        ));
    }
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        3,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "next.mkv".to_string(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "obfuscated.bin".to_string(),
                current_filename: "show.part03.rar".to_string(),
                canonical_filename: Some("show.part03.rar".to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(2),
                }),
                classification_source: FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 0);
    assert_eq!(work.priority, 3);
}

#[tokio::test]
async fn rar_unlock_bootstraps_volume_prefix_before_plan_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40140);
    let spec = JobSpec {
        name: "RAR Unlock Bootstrap".to_string(),
        password: None,
        total_bytes: 32,
        category: None,
        metadata: Vec::new(),
        files: (0..4)
            .map(|index| FileSpec {
                filename: format!("obfuscated-{index}.bin"),
                role: FileRole::Unknown,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 8,
                    message_id: format!("rar-unlock-bootstrap-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        for index in 0..4 {
            state.download_queue.push(rar_unlock_work(
                job_id,
                index,
                FileRole::Unknown.download_priority(),
            ));
        }
    }
    // PAR2 metadata classified every file, but no volume has completed yet:
    // no rar_sets plan exists, so only the bootstrap prefix selection can
    // pull volume 0 forward.
    for index in 0..4u32 {
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index: index,
                    source_filename: format!("obfuscated-{index}.bin"),
                    current_filename: format!("vol.part{:02}.rar", index + 1),
                    canonical_filename: Some(format!("vol.part{:02}.rar", index + 1)),
                    classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                        kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                        set_name: "vol".to_string(),
                        volume_index: Some(index),
                    }),
                    classification_source: FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let order = (0..4)
        .map(|_| state.download_queue.pop().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        order
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "volume 0 must lead the bootstrap prefix"
    );
    assert_eq!(
        order[0].priority, 1,
        "volume 0 queue entries are corrected to the protected base priority"
    );
    assert!(
        order[1..].iter().all(|work| work.priority == 3),
        "later prefix volumes ride the unlock tier"
    );
}

#[tokio::test]
async fn rar_unlock_does_not_boost_untrusted_topology_disagreement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40103);
    let spec = JobSpec {
        name: "RAR Unlock Untrusted Identity".to_string(),
        password: None,
        total_bytes: 8,
        category: None,
        metadata: Vec::new(),
        files: vec![FileSpec {
            filename: "not-the-topology-name.rar".to_string(),
            role: FileRole::Unknown,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 8,
                message_id: "rar-unlock-untrusted@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(rar_unlock_work(
            job_id,
            0,
            FileRole::Unknown.download_priority(),
        ));
    }
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        3,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "next.mkv".to_string(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "not-the-topology-name.rar".to_string(),
                current_filename: "not-the-topology-name.rar".to_string(),
                canonical_filename: None,
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(2),
                }),
                classification_source: FileIdentitySource::Probe,
            },
        )
        .unwrap();

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 0);
    assert_eq!(work.priority, FileRole::Unknown.download_priority());
}

#[tokio::test]
async fn rar_unlock_no_plan_or_topology_leaves_queue_unchanged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40104);
    let files = (0..3)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock No Topology", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[2]);

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 2);
    assert_eq!(work.priority, 12);
}

#[tokio::test]
async fn rar_unlock_missing_required_volume_without_queued_work_does_not_boost() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40105);
    let files = (0..4)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Missing Queued Volume", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        4,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "needs-missing-volume-two.mkv".to_string(),
            first_volume: 1,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 3);
    assert_eq!(work.priority, 13);
}

#[tokio::test]
async fn rar_unlock_counts_active_volume_as_near_unlock_prerequisite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40106);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Active Prerequisite", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3]);
    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id,
            file_index: 2,
        },
        1,
    );
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "needs-active-and-queued.mkv".to_string(),
            first_volume: 1,
            last_volume: 3,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 3);
    assert_eq!(work.priority, 3);
}

#[tokio::test]
async fn rar_unlock_ranked_window_uses_logical_volume_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40107);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Ranked Window", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[4, 2, 3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "wide-next.mkv".to_string(),
            first_volume: 1,
            last_volume: 4,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let popped = [
        state.download_queue.pop().unwrap(),
        state.download_queue.pop().unwrap(),
        state.download_queue.pop().unwrap(),
    ];
    assert_eq!(
        popped
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert!(popped.iter().all(|work| work.priority == 3));
}

#[tokio::test]
async fn rar_unlock_dirty_priorities_apply_before_lane_refill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40108);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Refill Dirty", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[4, 2, 3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "wide-next.mkv".to_string(),
            first_volume: 1,
            last_volume: 4,
            unpacked_size: 100,
        }],
    );
    pipeline.mark_rar_unlock_priorities_dirty(job_id);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: 3,
            is_recovery: false,
            completion_critical: false,
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            exclude_servers: Vec::new(),
            avoid_server: None,
        },
        response_tx,
    });

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("priority-3 RAR unlock lane should refill after reprioritization");
    assert_eq!(lease.job_id, job_id);
    assert_eq!(lease.works[0].segment_id.file_id.file_index, 2);
    assert_eq!(lease.works[0].priority, 3);
    assert!(!pipeline.rar_unlock_priority_dirty_jobs.contains(&job_id));
}

#[tokio::test]
async fn rar_unlock_retry_requeue_marks_rar_volume_dirty_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let rar_job_id = JobId(40109);
    let rar_files = (0..3)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let rar_spec = rar_job_spec("RAR Unlock Retry Dirty", &rar_files);
    insert_active_job(&mut pipeline, rar_job_id, rar_spec).await;
    pipeline.rar_unlock_priority_dirty_jobs.clear();

    pipeline.requeue_retry_work(rar_unlock_work(rar_job_id, 2, 12));
    assert!(
        pipeline
            .rar_unlock_priority_dirty_jobs
            .contains(&rar_job_id)
    );

    let standalone_job_id = JobId(40110);
    let standalone_spec = standalone_job_spec(
        "RAR Unlock Retry Non RAR",
        &[("standalone.bin".to_string(), 8)],
    );
    insert_active_job(&mut pipeline, standalone_job_id, standalone_spec).await;
    pipeline.rar_unlock_priority_dirty_jobs.clear();

    pipeline.requeue_retry_work(DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: standalone_job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        message_id: MessageId::new("rar-unlock-non-rar-retry@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: FileRole::Standalone.download_priority(),
        byte_estimate: 1024,
        retry_count: 1,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    });
    assert!(
        !pipeline
            .rar_unlock_priority_dirty_jobs
            .contains(&standalone_job_id)
    );
}

#[tokio::test]
async fn impossible_rar_state_fails_loudly_after_forced_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30076);
    let working_dir = intermediate_dir.join("impossible-rar-state");
    let mut state = minimal_job_state(job_id, "Impossible RAR State", working_dir);
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::new(),
        complete_volumes: HashSet::new(),
        expected_volume_count: None,
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "E10.mkv".to_string(),
            first_volume: 0,
            last_volume: 1,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(0u32, dummy_rar_volume_facts(0))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("invalid RAR state after recompute"));
}

#[tokio::test]
async fn extraction_queue_limits_to_tuner_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let extraction_limit = pipeline.tuner.max_concurrent_extractions();
    let jobs: Vec<JobId> = (0..=extraction_limit)
        .map(|idx| JobId(31101 + idx as u64))
        .collect();
    let queued_job = jobs[extraction_limit];

    for (idx, job_id) in jobs.iter().enumerate() {
        pipeline.jobs.insert(
            *job_id,
            minimal_job_state(
                *job_id,
                &format!("extract-{idx}"),
                temp_dir.path().join(format!("extract-{idx}")),
            ),
        );
    }

    for job_id in jobs.iter().take(extraction_limit) {
        assert!(pipeline.maybe_start_extraction(*job_id).await);
        assert_eq!(
            pipeline.jobs.get(job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting)
        );
    }
    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );

    assert!(!pipeline.maybe_start_extraction(queued_job).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::QueuedExtract)
    );

    pipeline.transition_postprocessing_status(jobs[0], JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![queued_job]
    );
}

#[tokio::test]
async fn queued_rar_extract_restart_relaunches_idle_ready_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31202);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Restore queued RAR extract", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set state should exist after all volumes complete");
        assert!(
            set_state
                .plan
                .as_ref()
                .is_some_and(|plan| !plan.ready_members.is_empty())
        );
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::QueuedExtract;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;

    let active_workers = pipeline
        .rar_sets
        .get(&key)
        .map(|state| state.active_workers)
        .unwrap_or_default();
    assert!(
        active_workers > 0,
        "idle restored QueuedExtract job should relaunch ready RAR work"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );

    for _ in 0..active_workers {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
}

#[tokio::test]
async fn extracting_rar_restart_with_failed_member_enters_repair_or_relaunches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31203);
    let files = build_multifile_multivolume_rar_set();
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let par2_bytes = build_test_par2_index(&files[0].0, &files[0].1, files[0].1.len() as u64);
    let recovery_bytes = vec![0xAA; files[0].1.len()];
    let mut all_files = files.clone();
    all_files.push((index_filename.to_string(), par2_bytes.clone()));
    all_files.push((recovery_filename.to_string(), recovery_bytes.clone()));
    let spec = rar_job_spec("Restore failed RAR repair", &all_files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    write_and_complete_file(
        &mut pipeline,
        job_id,
        files.len() as u32,
        index_filename,
        &par2_bytes,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        files.len() as u32 + 1,
        recovery_filename,
        &recovery_bytes,
    )
    .await;

    let mut damaged_first_volume = files[0].1.clone();
    damaged_first_volume[0] ^= 0xFF;
    tokio::fs::write(working_dir.join(&files[0].0), &damaged_first_volume)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(&files[0].0, &files[0].1, files[0].1.len() as u64, 1),
        &[
            (files.len() as u32, index_filename, 0, false),
            (files.len() as u32 + 1, recovery_filename, 1, true),
        ],
    );

    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set state should exist after all volumes complete");
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::AwaitingRepair;
        if let Some(plan) = set_state.plan.as_mut() {
            plan.phase = crate::pipeline::rar_state::RarSetPhase::AwaitingRepair;
        }
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;
    // The move into repair is decided by the detached damaged-path read.
    settle_par2_analysis_work(&mut pipeline).await;

    let active_workers = pipeline
        .rar_sets
        .get(&key)
        .map(|state| state.active_workers)
        .unwrap_or_default();
    let status = pipeline.jobs.get(&job_id).map(|state| state.status.clone());
    let completion_check_pending = pipeline.pending_completion_checks.contains(&job_id);

    assert!(
        !matches!(status, Some(JobStatus::Failed { .. })),
        "restart repair liveness must not terminally fail; status={status:?}"
    );

    assert!(
        active_workers > 0
            || matches!(
                status,
                Some(JobStatus::Repairing | JobStatus::QueuedRepair | JobStatus::Complete)
            )
            || completion_check_pending,
        "stale idle failed RAR extraction must enter repair or relaunch extraction; status={status:?}, active_workers={active_workers}"
    );

    for _ in 0..active_workers {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
}

#[tokio::test]
async fn unacceptable_rar_member_is_rejected_before_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31203);
    let mut files = build_multifile_multivolume_rar_set();
    for (_, bytes) in &mut files {
        if let Some(offset) = bytes
            .windows(b"E01.mkv".len())
            .position(|window| window == b"E01.mkv")
        {
            bytes[offset..offset + b"E01.mkv".len()].copy_from_slice(b"E01.exe");
        }
    }
    pipeline
        .db
        .save_post_processing_settings(&crate::post_processing::model::PostProcessingSettings {
            unacceptable_extensions: vec!["exe".into()],
            ..Default::default()
        })
        .unwrap();
    let spec = rar_job_spec("RAR Header Rejection", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    assert_eq!(pipeline.extract_rar_set(job_id, "show").await.unwrap(), 0);
    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(|sets| !sets.contains("show"))
    );
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    let JobStatus::Failed { error } = status else {
        panic!("a confirmed RAR member extension must fail the job");
    };
    assert!(error.contains("RAR member 'E01.exe' before extraction"));
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(
                "RAR Header Rejection"
            ))
            .exists()
    );
}

#[tokio::test]
async fn direct_full_set_rar_extraction_registers_and_blocks_incremental_batches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31204);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Direct full-set RAR ownership", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline
        .extract_rar_set(job_id, "show")
        .await
        .expect("direct full-set extraction should launch");
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| sets.contains("show")),
        "direct full-set extraction must register set ownership"
    );

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain while full-set extraction is active");
    assert_eq!(set_state.active_workers, 1);
    assert!(
        set_state.in_flight_members.is_empty(),
        "incremental batches must not start for a set owned by direct full-set extraction"
    );

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn active_full_set_rar_extraction_blocks_restart_repair_and_relaunch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31208);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Active full-set RAR restart guard", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .extract_rar_set(job_id, "show")
        .await
        .expect("direct full-set extraction should launch");
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 0);
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(
        pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv"))
    );
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain while full-set extraction is active");
    assert_eq!(set_state.active_workers, 1);
    assert!(set_state.in_flight_members.is_empty());
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| sets.contains("show"))
    );

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn reconcile_job_progress_marks_waiting_for_rar_volumes_without_clobbering_download_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31233);
    let set_name = "show".to_string();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "waiting-rar", temp_dir.path().join("waiting-rar")),
    );
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            set_name.clone(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: std::collections::HashMap::from([
                    ("show.part01.rar".to_string(), 0u32),
                    ("show.part02.rar".to_string(), 1u32),
                    ("show.part03.rar".to_string(), 2u32),
                ]),
                complete_volumes: std::collections::HashSet::from([0u32, 1u32]),
                expected_volume_count: Some(3),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "E01.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 2,
                    unpacked_size: 100,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            facts_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::from([2u32]),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: pipeline
                    .jobs
                    .get(&job_id)
                    .unwrap()
                    .assembly
                    .archive_topology_for(&set_name)
                    .unwrap()
                    .clone(),
                fallback_reason: None,
            }),
        },
    );

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        state.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(state.post_state, crate::jobs::model::PostState::Idle);
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn purge_idle_rar_set_drops_shared_kdf_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31244);
    let set_name = "show".to_string();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "purge-job", temp_dir.path().join("purge-job")),
    );

    let shared_kdf_cache = std::sync::Arc::new(unrar_rs::crypto::KdfCache::new());
    let weak_cache = std::sync::Arc::downgrade(&shared_kdf_cache);

    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        rar_state::RarSetState {
            shared_kdf_cache,
            ..Default::default()
        },
    );

    pipeline.purge_empty_rar_set_if_idle(job_id, &set_name);

    assert!(!pipeline.rar_sets.contains_key(&(job_id, set_name.clone())));
    assert!(weak_cache.upgrade().is_none());
}

#[tokio::test]
async fn repaired_middle_volume_refreshes_topology_despite_an_existing_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30240);
    let files = build_multifile_multivolume_rar_set();
    complete_rar_set_with_plan(&mut pipeline, job_id, &files, "Repaired Middle Volume").await;

    let verification = all_complete_verification(&files);

    // The trap this fix exists for: the plan exists, so nothing is judged to
    // need a refresh, and no file here is `Renamed`.
    assert!(
        pipeline
            .verified_complete_archive_file_ids_needing_refresh(
                job_id,
                &verification,
                &HashSet::new(),
            )
            .is_empty(),
        "an existing plan suppresses every refresh when the repair is not named"
    );

    // Volume 1 is interior: the member E01.mkv starts at volume 0 and continues
    // past it, so a chain rebuilt without volume 1's repaired header truncates
    // the member and extraction opens a short volume range.
    let rewritten: HashSet<par2_rs::FileId> = [verification.files[1].file_id].into_iter().collect();
    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(
            job_id,
            &verification,
            &rewritten,
        ),
        vec![NzbFileId {
            job_id,
            file_index: 1
        }],
        "the volume the repair rewrote must refresh even though its set has a plan"
    );
}

#[tokio::test]
async fn repaired_final_volume_refreshes_topology_despite_an_existing_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30241);
    let files = build_multifile_multivolume_rar_set();
    complete_rar_set_with_plan(&mut pipeline, job_id, &files, "Repaired Final Volume").await;

    let verification = all_complete_verification(&files);
    // The boundary case: a stale chain that stops at the last volume is
    // accidentally the right length, so nothing downstream would notice the
    // missed refresh. It must still happen — the repaired header is what proves
    // the member ends there rather than merely appearing to.
    let last = files.len() - 1;
    let rewritten: HashSet<par2_rs::FileId> =
        [verification.files[last].file_id].into_iter().collect();
    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(
            job_id,
            &verification,
            &rewritten,
        ),
        vec![NzbFileId {
            job_id,
            file_index: last as u32
        }],
        "a repaired final volume must refresh like any other"
    );
}
