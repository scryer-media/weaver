//! `rar_extraction` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn par2_metadata_immediately_rebinds_obfuscated_rar_file_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30111);
    let canonical_filename = "show.part001.rar";
    let obfuscated_filename = "51273aad56a8b904e96928935278a627.101";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let par2_filename = "repair.par2";
    let par2_bytes = build_test_par2_index(canonical_filename, &rar_bytes, 8);
    let spec = JobSpec {
        name: "PAR2 Canonical Rebind".to_string(),
        password: None,
        total_bytes: (rar_bytes.len() + par2_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: obfuscated_filename.to_string(),
                role: FileRole::from_filename(obfuscated_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: rar_bytes.len() as u32,
                    message_id: "rar-obfuscated@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: par2_filename.to_string(),
                role: FileRole::from_filename(par2_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 0, obfuscated_filename, &rar_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 1, par2_filename, &par2_bytes).await;
    pipeline
        .try_load_par2_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 1,
            },
        )
        .await;
    drain_rar_refreshes(&mut pipeline).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("data file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(matches!(
        identity
            .classification
            .as_ref()
            .map(|classification| &classification.kind),
        Some(crate::jobs::assembly::DetectedArchiveKind::Rar)
    ));
    assert!(!working_dir.join(obfuscated_filename).exists());
    assert!(working_dir.join(canonical_filename).exists());

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("PAR2 rebinding should rebuild RAR topology");
    assert!(topology.volume_map.contains_key(canonical_filename));
    assert!(!topology.volume_map.contains_key(obfuscated_filename));
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn authoritative_par2_identity_clears_preexisting_stale_rar_set_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30131);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Authoritative PAR2 Identity Stale Set",
            &[(old_filename.to_string(), rar_bytes.clone())],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, old_set_name.to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(old_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: vec![crate::jobs::assembly::ArchiveMember {
                        name: "E01.mkv".to_string(),
                        first_volume: 0,
                        last_volume: 1,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes)]),
        &[],
    );

    pipeline.retry_par2_authoritative_identity(job_id).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity
            .classification
            .as_ref()
            .map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, old_set_name.to_string()))
    );
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
}

#[tokio::test]
async fn restore_job_scrubs_stale_par2_rar_set_state_before_rar_runtime_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30132);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = rar_job_spec(
        "Restore Stale PAR2 RAR Set",
        &[(old_filename.to_string(), rar_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    write_and_complete_file(&mut pipeline, job_id, 0, old_filename, &rar_bytes).await;
    persist_completed_file_hash(&pipeline, job_id, 0, old_filename, &rar_bytes).await;
    std::fs::rename(
        working_dir.join(old_filename),
        working_dir.join(canonical_filename),
    )
    .unwrap();

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    let encoded_facts = rmp_serde::to_vec_named(&dummy_rar_volume_facts(0)).unwrap();
    pipeline
        .db
        .save_rar_volume_facts(job_id, old_set_name, 0, &encoded_facts)
        .unwrap();

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: spec.clone(),
            file_progress: recovered.file_progress,
            complete_files: recovered.complete_files,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let identity = restored
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist after restore");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity
            .classification
            .as_ref()
            .map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(
        !restored
            .rar_sets
            .contains_key(&(job_id, old_set_name.to_string()))
    );
    assert!(
        restored
            .rar_sets
            .contains_key(&(job_id, "show".to_string()))
    );
    assert!(
        !restored
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .contains_key(old_set_name)
    );
}

#[tokio::test]
async fn par2_metadata_rebinds_obfuscated_rar_after_late_content_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30112);
    let canonical_files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = canonical_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 101),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("PAR2 Late Canonical Rebind", &obfuscated_files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&canonical_files),
        &[],
    );

    for (index, ((_, _), (obfuscated_filename, bytes))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        write_and_complete_file(
            &mut pipeline,
            job_id,
            index as u32,
            obfuscated_filename,
            bytes,
        )
        .await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    for (index, ((canonical_filename, _), (obfuscated_filename, _))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        let identity = pipeline
            .file_identity(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: index as u32,
                },
            )
            .cloned()
            .expect("data file identity should exist");
        assert_eq!(identity.current_filename, *canonical_filename);
        assert_eq!(
            identity.canonical_filename.as_deref(),
            Some(canonical_filename.as_str())
        );
        assert_eq!(identity.classification_source, FileIdentitySource::Par2);
        assert!(!working_dir.join(obfuscated_filename).exists());
        assert!(working_dir.join(canonical_filename).exists());
    }

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("late PAR2 rebinding should rebuild RAR topology");
    for (canonical_filename, _) in &canonical_files {
        assert!(topology.volume_map.contains_key(canonical_filename));
    }
    for (obfuscated_filename, _) in &obfuscated_files {
        assert!(!topology.volume_map.contains_key(obfuscated_filename));
    }
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive late canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn waiting_for_missing_volumes_ignores_stale_noncurrent_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30129);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Ignore Stale RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xAB; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "51273aad56a8b904e96928935278a627.101".to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "51273aad56a8b904e96928935278a627".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(
                        "51273aad56a8b904e96928935278a627.101".to_string(),
                        0,
                    )]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn waiting_for_missing_volumes_still_tracks_current_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30130);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Track Current RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xCD; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: canonical_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(canonical_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_rar_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30113);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Clean PAR2 RAR Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(done_job_id, job_id);
            assert!(!attempted.is_empty());
            assert!(result.is_ok());
        }
        _ => panic!("expected RAR extraction batch"),
    }
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30116);
    let files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let spec = rar_job_spec("Clean PAR2 7z Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            result,
            ..
        } => {
            assert_eq!(*done_job_id, job_id);
            if let Err(error) = result {
                panic!("7z extraction failed: {error}");
            }
        }
        _ => panic!("expected 7z extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_single_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30117);
    let archive_filename = "archive.7z";
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = standalone_job_spec(
        "Clean PAR2 Single 7z Verify Starts Extraction",
        &[(archive_filename.to_string(), seven_zip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), seven_zip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    persist_completed_file_hash(&pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
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

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_err());
        }
        _ => panic!("expected single 7z extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_gzip_extraction() {
    use std::io::Write;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30118);
    let archive_filename = "payload.gz";
    let payload = b"gzip finalize payload";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).unwrap();
    let gzip_bytes = encoder.finish().unwrap();
    let spec = standalone_job_spec(
        "Clean PAR2 Gzip Verify Starts Extraction",
        &[(archive_filename.to_string(), gzip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), gzip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &gzip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Gz,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "payload".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: payload.len() as u64,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_ok());
        }
        _ => panic!("expected gzip extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn restore_job_does_not_rehydrate_lossy_extraction_attempt_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Restart Runtime Restore", &files);
    let job_id = JobId(30034);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .db
            .add_failed_extraction(job_id, "E10.mkv")
            .unwrap();
        pipeline
            .db
            .add_failed_extraction(job_id, "E15.mkv")
            .unwrap();
        pipeline
            .db
            .set_active_job_normalization_retried(job_id, true)
            .unwrap();
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::from([
                NzbFileId {
                    job_id,
                    file_index: 0,
                },
                NzbFileId {
                    job_id,
                    file_index: 1,
                },
            ]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert!(!restored.failed_extractions.contains_key(&job_id));
    assert!(restored.normalization_retried.contains(&job_id));
    assert!(
        restored
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .is_none_or(|state| state.verified_suspect_volumes.is_empty())
    );
}

#[tokio::test]
async fn normalization_refresh_rebuilds_rar_snapshot_from_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30004);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalize Refresh", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in [
        (0usize, &files[0]),
        (1, &files[1]),
        (3, &files[3]),
        (2, &files[2]),
    ] {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::write(working_dir.join("show.part03.rar"), &files[2].1)
        .await
        .unwrap();
    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string()].into_iter().collect(),
        )
        .await
        .unwrap();

    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .contains_key("show")
    );
}

#[tokio::test]
async fn live_rebuild_failure_retains_previous_rar_volume_facts_and_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30005);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebuild Failure", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let original_facts = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .cloned()
        .expect("good facts should be persisted");

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part01.rar").exists());
    assert!(working_dir.join("show.part02.rar").exists());

    let corrupt_part04 = vec![0u8; files[3].1.len()];
    tokio::fs::write(working_dir.join(&files[3].0), &corrupt_part04)
        .await
        .unwrap();

    pipeline
        .try_update_archive_topology(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .await;

    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .cloned(),
        Some(original_facts)
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
}

#[tokio::test]
async fn incremental_rar_batches_survive_eager_delete_of_earlier_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30006);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Batches", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let first_done = next_extraction_done(&mut pipeline).await;
    match &first_done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(first_done).await;

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;
    pipeline.try_rar_extraction(job_id).await;

    let second_done = next_extraction_done(&mut pipeline).await;
    match &second_done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(second_done).await;
    settle_inflight_moves(&mut pipeline).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn non_solid_incremental_rar_batches_cleanup_chunks_after_finalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30007);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Chunks", &files);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let staging_dir = pipeline.extraction_staging_dir(job_id);

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    let chunks = pipeline.db.get_extraction_chunks(job_id, "show").unwrap();
    assert!(chunks.iter().all(|chunk| chunk.member_name != "E01.mkv"));
    assert!(staging_dir.join("E01.mkv").exists());
    assert!(
        !staging_dir
            .join(".weaver-chunks")
            .join("show")
            .join("E01.mkv")
            .exists()
    );
}

#[tokio::test]
async fn non_solid_rar_set_dispatches_two_members_concurrently() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30008);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Concurrent Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(
        attempted_members,
        vec!["E01.mkv".to_string(), "E02.mkv".to_string()]
    );

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn rar_eager_delete_waits_for_all_active_workers_in_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30009);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Delete Waits For Workers", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(2)
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;

    pipeline.handle_extraction_done(first_done).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(1)
    );
    for (filename, _) in &files {
        assert!(
            working_dir.join(filename).exists(),
            "{filename} should not be eagerly deleted while another set worker is active"
        );
    }

    pipeline.handle_extraction_done(second_done).await;
    for (filename, _) in &files {
        assert!(
            !working_dir.join(filename).exists(),
            "{filename} should be eagerly deleted after all set workers finish"
        );
    }
}

#[tokio::test]
async fn non_solid_rar_scheduler_skips_duplicate_ready_members() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30010);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Duplicate Ready Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("set state should exist");
    let plan = set_state
        .plan
        .as_mut()
        .expect("ready plan should exist after all volumes complete");
    let first = plan.ready_members[0].name.clone();
    let second = plan.ready_members[1].name.clone();
    plan.ready_members = vec![
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: second.clone(),
        },
    ];

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        [first.clone(), second.clone()].into_iter().collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(attempted_members, vec![first, second]);

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn non_solid_rar_scheduler_waits_for_link_dependency_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30011);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Link Dependency Source", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_key = (job_id, "show".to_string());
    let (source_member, dependent_member, source_first_volume, source_last_volume) = {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("set state should exist");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all volumes complete");
        let source_member = plan.ready_members[0].name.clone();
        let dependent_member = plan.ready_members[1].name.clone();
        let source = plan
            .topology
            .members
            .iter()
            .find(|member| member.name == source_member)
            .expect("source member should be present in topology");
        let source_first_volume = source.first_volume;
        let source_last_volume = source.last_volume;
        plan.member_dependencies.insert(
            dependent_member.clone(),
            crate::pipeline::rar_state::RarMemberDependency {
                source_member: source_member.clone(),
                source_first_volume,
                source_last_volume,
            },
        );
        plan.ready_members = vec![
            crate::pipeline::rar_state::RarReadyMember {
                name: source_member.clone(),
            },
            crate::pipeline::rar_state::RarReadyMember {
                name: dependent_member.clone(),
            },
        ];
        (
            source_member,
            dependent_member,
            source_first_volume,
            source_last_volume,
        )
    };
    pipeline.extracted_members.insert(job_id, HashSet::new());
    resume_job_downloading_for_test(&mut pipeline, job_id);
    assert_eq!(
        pipeline
            .rar_sets
            .get(&set_key)
            .and_then(|set| set.plan.as_ref())
            .and_then(|plan| plan.member_dependencies.get(&dependent_member))
            .map(|dependency| {
                (
                    dependency.source_first_volume,
                    dependency.source_last_volume,
                )
            }),
        Some((source_first_volume, source_last_volume))
    );

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        [source_member.clone()].into_iter().collect()
    );

    let source_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(source_done).await;
    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        [dependent_member].into_iter().collect()
    );
}

#[tokio::test]
async fn rar_identity_rebind_preserves_in_flight_workers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30012);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Preserves Workers", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let expected_in_flight: std::collections::HashSet<String> =
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect();
    let set_key = (job_id, "show".to_string());
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);
    let generation_before_rebind = set_state.extraction_generation;

    let stale_headers = shortened_e01_rar_headers(&files);
    pipeline.rar_sets.get_mut(&set_key).unwrap().cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();
    assert!(
        pipeline.load_rar_snapshot(job_id, "show").is_some(),
        "the shortened topology should be cached in memory and the database"
    );

    let touched_filenames = [files[0].0.clone()].into_iter().collect();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive identity rebind");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);
    assert!(set_state.plan.is_none());
    assert_eq!(
        set_state.extraction_generation,
        generation_before_rebind.saturating_add(1)
    );
    assert_eq!(
        pipeline.load_rar_snapshot(job_id, "show").as_deref(),
        Some(stale_headers.as_slice()),
        "identity rebinding retains the snapshot as a rebuild base without retaining its plan"
    );

    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist after completions");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());
}

#[tokio::test]
async fn rar_identity_rebind_removes_empty_set_after_in_flight_workers_finish() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Removes Empty Set", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_key = (job_id, "show".to_string());
    let touched_filenames = files
        .iter()
        .map(|(filename, _)| filename.clone())
        .collect::<HashSet<_>>();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive rebind while extraction workers are still running");
    assert!(set_state.volume_files.is_empty());
    assert!(set_state.active_workers > 0);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;

    assert!(
        !pipeline.rar_sets.contains_key(&set_key),
        "fully invalidated RAR set should be purged once in-flight workers finish"
    );
}

#[tokio::test]
async fn non_solid_rar_incremental_requires_member_chain_not_download_activity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Readiness", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    assert!(!pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after first RAR volume");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());

    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after second RAR volume");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string()].into_iter().collect()
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_extraction_capacity_pressure_keeps_member_waiting_without_repair_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Extraction Capacity Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist after ready RAR member");
        set_state.active_workers = 1;
        set_state.in_flight_members.insert("E01.mkv".to_string());
    }

    let capacity_error = format!(
        "{}: synthetic EMFILE",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );
    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Err(capacity_error.clone()),
        })
        .await;

    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain after capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(!set_state.in_flight_members.contains("E01.mkv"));
    assert!(
        !pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv")),
        "capacity pressure must not mark the member failed"
    );
    let pending_key = (job_id, "show".to_string(), RarCapacityRetryKind::Extraction);
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist for duplicate capacity retry");
        set_state.active_workers = 1;
        set_state.in_flight_members.insert("E01.mkv".to_string());
    }
    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Err(capacity_error),
        })
        .await;
    assert_eq!(
        pipeline
            .pending_rar_capacity_retries
            .iter()
            .filter(|key| **key == pending_key)
            .count(),
        1
    );

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain blocked by pending capacity retry");
    assert_eq!(set_state.active_workers, 0);
    assert!(!set_state.in_flight_members.contains("E01.mkv"));

    pipeline
        .handle_rar_capacity_retry(RarCapacityRetry {
            job_id,
            set_name: "show".to_string(),
            kind: RarCapacityRetryKind::Extraction,
        })
        .await;
    assert!(!pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain after extraction retry wakeup");
    assert_eq!(set_state.active_workers, 1);
    assert!(set_state.in_flight_members.contains("E01.mkv"));

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_full_set_capacity_pressure_retries_without_failing_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Full Set Capacity Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_name = "show".to_string();
    let key = (job_id, set_name.clone());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist after RAR volumes complete");
        set_state.active_workers = 1;
        set_state.in_flight_members.clear();
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        let plan = set_state
            .plan
            .as_mut()
            .expect("RAR plan should exist after volume facts are built");
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert(set_name.clone());

    let capacity_error = format!(
        "{}: synthetic EMFILE",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );
    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Err(capacity_error),
        })
        .await;

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(|sets| !sets.contains(&set_name))
    );
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain after full-set capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
    ));
    assert!(matches!(
        set_state.plan.as_ref().map(|plan| plan.phase),
        Some(crate::pipeline::rar_state::RarSetPhase::FallbackFullSet)
    ));
    let pending_key = (
        job_id,
        set_name.clone(),
        RarCapacityRetryKind::FullSetExtraction,
    );
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline
        .handle_rar_capacity_retry(RarCapacityRetry {
            job_id,
            set_name: set_name.clone(),
            kind: RarCapacityRetryKind::FullSetExtraction,
        })
        .await;
    assert!(!pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should be extracting after full-set retry wakeup");
    assert_eq!(set_state.active_workers, 1);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::Extracting
    ));

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_full_set_member_capacity_pressure_retries_without_repair_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40127);
    let set_name =
        setup_extracting_rar_full_set(&mut pipeline, job_id, "RAR Full Set Member Capacity Retry")
            .await;
    let capacity_error = format!(
        "{}: synthetic EMFILE during read",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Ok(FullSetExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: vec![("E02.mkv".to_string(), capacity_error)],
                selected_password: None,
            }),
        })
        .await;

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(
        !pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E02.mkv")),
        "capacity pressure must not mark the failed member for repair"
    );
    let pending_key = (
        job_id,
        set_name.clone(),
        RarCapacityRetryKind::FullSetExtraction,
    );
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, set_name))
        .expect("RAR set should remain after member capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
    ));
}

#[tokio::test]
async fn rar_full_set_mixed_failures_defer_repair_when_capacity_pressure_is_present() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40128);
    let set_name =
        setup_extracting_rar_full_set(&mut pipeline, job_id, "RAR Full Set Mixed Capacity Retry")
            .await;
    let capacity_error = format!(
        "{}: synthetic EMFILE during read",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Ok(FullSetExtractionOutcome {
                extracted: Vec::new(),
                failed: vec![
                    ("E01.mkv".to_string(), capacity_error),
                    ("E02.mkv".to_string(), "Invalid checksum".to_string()),
                ],
                selected_password: None,
            }),
        })
        .await;

    assert!(
        !pipeline.failed_extractions.contains_key(&job_id),
        "mixed capacity passes must not promote any member until a capacity-free retry confirms it"
    );
    let pending_key = (job_id, set_name, RarCapacityRetryKind::FullSetExtraction);
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));
}

#[tokio::test]
async fn non_solid_rar_incremental_uses_ready_plan_even_if_complete_volumes_lag() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Ready Plan Wins", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("set state should exist after all RAR volumes complete");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all RAR volumes complete");
        plan.ready_members = vec![crate::pipeline::rar_state::RarReadyMember {
            name: "E02.mkv".to_string(),
        }];
        plan.topology.complete_volumes = [2u32].into_iter().collect();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut topology = state
            .assembly
            .archive_topology_for("show")
            .cloned()
            .expect("assembly topology should exist for completed RAR set");
        topology.complete_volumes = [2u32].into_iter().collect();
        state
            .assembly
            .set_archive_topology("show".to_string(), topology);
    }

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E02.mkv"));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist while retry is in flight");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        ["E02.mkv".to_string()].into_iter().collect()
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
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn solid_rar_keeps_later_members_ready_after_earlier_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30009);
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5/rar5_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();
    let archive = unrar_rs::RarArchive::open(std::fs::File::open(&fixture_path).unwrap()).unwrap();
    let member_names = archive.member_names();
    assert!(member_names.len() >= 3);

    let spec = rar_job_spec(
        "Solid Failure Continuation",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    // Rebuild with extracted + failed state that mirrors a solid archive
    // where earlier members were attempted before later members.
    pipeline
        .extracted_members
        .insert(job_id, [member_names[0].to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, [member_names[1].to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "solid")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "solid".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("solid set plan should exist");

    assert!(plan.is_solid);
    assert_eq!(plan.phase, crate::pipeline::rar_state::RarSetPhase::Ready);
    let ready_members: Vec<String> = plan
        .ready_members
        .into_iter()
        .map(|member| member.name)
        .collect();
    assert_eq!(
        ready_members,
        member_names[2..]
            .iter()
            .map(|member| member.to_string())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn solid_rar4_pipeline_extracts_large_fixture_end_to_end() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30010);
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar4/rar4_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();

    let spec = rar_job_spec(
        "RAR4 Solid End-to-End",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR4 Solid End-to-End",
    ));
    let dest_entries = std::fs::read_dir(&dest)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let working_entries = std::fs::read_dir(&working_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "job status: {:?}, dest entries: {:?}, working entries: {:?}",
        job_status_for_assert(&pipeline, job_id),
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("sample.mkv").exists(),
        "dest entries: {:?}, working entries: {:?}",
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("file1.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(
        dest.join("file2.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(!working_dir.join("solid.rar").exists());
}

#[tokio::test]
async fn check_job_completion_retains_par2_until_rar_extraction_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Keeps PAR2 During Extraction", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(4).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    write_and_complete_file(&mut pipeline, job_id, 4, par2_filename, &files[4].1).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after volume facts are built");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Extracting;

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.recovery_blocks),
        Some(1)
    );
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn check_job_completion_defers_verify_while_rar_workers_are_active() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Barrier", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }

    // Set status to Extracting to match what the real pipeline would have
    // done when extraction workers were spawned.  The bounded workload
    // queue may gate through QueuedExtract first, but by the time workers
    // are active the status is always Extracting.
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Extracting;
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!pipeline.normalization_retried.contains(&job_id));

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 0;
    set_state.in_flight_members.insert("E02.mkv".to_string());
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn partial_rar_extraction_does_not_bypass_par2_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30012);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Partial Keeps PAR2", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    assert!(!pipeline.par2_bypassed.contains(&job_id));
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn rar_completion_prefers_incremental_batches_over_full_set_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Completion Uses Batch", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();
    pipeline.try_delete_volumes(job_id, "show");

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

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
        ExtractionDone::FullSet { .. } => {
            panic!("RAR completion should not fall back to full-set extraction here")
        }
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn eager_delete_exclusions_do_not_hide_suspect_deleted_rar_damage() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Keeps Suspect Deleted Volumes", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part02.rar".to_string(), "show.part04.rar".to_string()]
            .into_iter()
            .collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let mut verification = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: par2_rs::FileId::from_bytes([1; 16]),
                filename: "show.part02.rar".to_string(),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false; 3],
                missing_slice_count: 3,
            },
            par2_rs::verify::FileVerification {
                file_id: par2_rs::FileId::from_bytes([2; 16]),
                filename: "show.part04.rar".to_string(),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false; 2],
                missing_slice_count: 2,
            },
        ],
        recovery_blocks_available: 3,
        total_missing_blocks: 5,
        repairable: par2_rs::verify::Repairability::Insufficient {
            blocks_needed: 5,
            blocks_available: 3,
            deficit: 2,
        },
    };

    let (skipped_blocks, retained_suspect_blocks) =
        pipeline.apply_eager_delete_exclusions(job_id, &mut verification);

    assert_eq!(skipped_blocks, 2);
    assert_eq!(retained_suspect_blocks, 3);
    assert_eq!(verification.total_missing_blocks, 3);
    assert!(matches!(
        verification.files[0].status,
        par2_rs::verify::FileStatus::Missing
    ));
    assert_eq!(verification.files[0].missing_slice_count, 3);
    assert!(matches!(
        verification.files[1].status,
        par2_rs::verify::FileStatus::Complete
    ));
    assert_eq!(verification.files[1].missing_slice_count, 0);
    assert!(matches!(
        verification.repairable,
        par2_rs::verify::Repairability::Repairable {
            blocks_needed: 3,
            blocks_available: 3
        }
    ));

    pipeline.recompute_volume_safety_from_verification(job_id, &verification);

    let verified_suspect = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .unwrap_or_default();
    assert!(verified_suspect.contains(&1));
    assert!(!verified_suspect.contains(&3));
}

#[tokio::test]
async fn recoverable_full_set_extraction_error_defers_to_repair_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = standalone_job_spec(
        "Recoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to extract sample.mkv: Invalid checksum".to_string()),
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.failed_extractions.get(&job_id),
        Some(&HashSet::from(["archive.zip".to_string()]))
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );
}

#[tokio::test]
async fn nonrecoverable_full_set_extraction_error_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let spec = standalone_job_spec(
        "Nonrecoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to parse zip central directory".to_string()),
        })
        .await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn non_rar_full_set_capacity_pressure_still_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30026);
    let spec = standalone_job_spec(
        "Non-RAR Capacity Pressure Still Fails",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err(format!(
                "{}: synthetic EMFILE",
                crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
            )),
        })
        .await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(pipeline.pending_rar_capacity_retries.is_empty());
}

#[tokio::test]
async fn clean_verify_retries_non_rar_full_set_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec(
        "Non-RAR Extraction Retry After Verify",
        &[("archive.zip".to_string(), 16)],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join("archive.zip"), b"not-a-real-zip")
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 14)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.zip".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([("archive.zip".to_string(), 0)]),
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

    install_test_par2_runtime(&mut pipeline, job_id, minimal_par2_file_set(), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["archive.zip".to_string()].into_iter().collect());

    pipeline.check_job_completion(job_id).await;

    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet { set_name, .. } => {
            assert_eq!(set_name, "archive.zip");
        }
        _ => panic!("expected full-set extraction retry"),
    }
}

#[tokio::test]
async fn rar_state_recompute_supplements_stale_volume_registry_from_assembly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Registry Merge", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .volume_files = std::collections::BTreeMap::from([(0u32, "show.part01.rar".to_string())]);

    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(volume_paths.len(), 4);
    assert!(volume_paths.contains_key(&0));
    assert!(volume_paths.contains_key(&1));
    assert!(volume_paths.contains_key(&2));
    assert!(volume_paths.contains_key(&3));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert_eq!(plan.topology.complete_volumes.len(), 4);
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 1));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 2));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 3));
}

#[tokio::test]
async fn incremental_rar_member_extraction_uses_member_span_volume_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Member Span Window", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let all_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(all_paths.len(), 4);

    let member_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        false,
    );
    assert_eq!(member_paths.keys().copied().collect::<Vec<_>>(), vec![2, 3]);

    {
        let plan = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_mut())
            .expect("RAR plan should exist");
        plan.member_dependencies.insert(
            "E02.mkv".to_string(),
            crate::pipeline::rar_state::RarMemberDependency {
                source_member: "E01.mkv".to_string(),
                source_first_volume: 0,
                source_last_volume: 1,
            },
        );
    }
    let filecopy_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        false,
    );
    assert_eq!(
        filecopy_paths.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );

    let without_cached_headers = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        false,
        false,
    );
    assert_eq!(without_cached_headers.len(), all_paths.len());

    let solid_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        true,
    );
    assert_eq!(solid_paths.len(), all_paths.len());
}

#[tokio::test]
async fn generic_par2_repair_requeues_extraction_for_7z_and_gzip_payloads() {
    for (job_id, payload_filename, original_payload) in [
        (
            JobId(30082),
            "archive.7z",
            vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04],
        ),
        (
            JobId(30083),
            "payload.gz",
            vec![0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03],
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let index_filename = "repair.par2";
        let recovery_filename = "repair.vol00+01.par2";
        let damaged_payload = vec![0u8; original_payload.len()];
        let par2_bytes = build_test_par2_index(
            payload_filename,
            &original_payload,
            original_payload.len() as u64,
        );
        let recovery_bytes = vec![0xCC; original_payload.len()];
        let spec = JobSpec {
            name: format!("Archive Repair {}", job_id.0),
            password: None,
            total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: payload_filename.to_string(),
                    role: FileRole::from_filename(payload_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: original_payload.len() as u32,
                        message_id: format!("archive-{}-0@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: index_filename.to_string(),
                    role: FileRole::from_filename(index_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: par2_bytes.len() as u32,
                        message_id: format!("archive-{}-index@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: recovery_filename.to_string(),
                    role: FileRole::from_filename(recovery_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: recovery_bytes.len() as u32,
                        message_id: format!("archive-{}-recovery@example.com", job_id.0),
                    }],
                },
            ],
        };
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
            .await
            .unwrap();
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }
        write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
        write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set(
                payload_filename,
                &original_payload,
                original_payload.len() as u64,
                1,
            ),
            &[
                (1, index_filename, 0, false),
                (2, recovery_filename, 1, true),
            ],
        );

        pipeline.check_job_completion(job_id).await;
        // The repair the analysis decides only happens once the detached read's
        // verdict lands, and the post-repair check is scheduled from there.
        settle_par2_analysis_work(&mut pipeline).await;

        let queued_job = pipeline
            .pending_completion_checks
            .pop_front()
            .expect("post-repair completion should be scheduled");
        assert_eq!(queued_job, job_id);
        pipeline.check_job_completion(queued_job).await;

        assert!(matches!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting | JobStatus::QueuedExtract)
        ));
        let done = next_extraction_done(&mut pipeline).await;
        match done {
            ExtractionDone::FullSet { set_name, .. } => {
                assert_eq!(set_name, payload_filename);
            }
            _ => panic!("expected full-set extraction after generic PAR2 repair"),
        }
    }
}

/// A split 7z set short one part the NZB never carried is repaired from its
/// recovery blocks and then extracted — for an interior part and for the last
/// part alike.
///
/// Before this, neither shape ever reached PAR2. The interior hole left the
/// topology short of ready forever, with no predicate naming the absence the
/// way the RAR scheduler's `WaitingForVolumes` does; the missing last part was
/// not even known to be missing, so the strong-decode fast path settled the set
/// as clean and extraction opened a truncated set. And had a repair somehow
/// run, the rebuilt part sat on disk under a name the assembly had never heard
/// of, outside the topology and outside `archive_set_part_paths`.
#[tokio::test]
async fn par2_rebuilds_a_split_7z_part_the_nzb_never_carried_and_extraction_follows() {
    for (job_id, withheld) in [(JobId(30084), 2usize), (JobId(30085), 6usize)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let set_name = "generated_split_store_plain.7z";
        let parts = sevenz_fixture_bytes(set_name);
        assert_eq!(parts.len(), 7, "fixture has seven parts");
        let (withheld_name, withheld_bytes) = parts[withheld].clone();
        let index_filename = "silver_horizon.par2";
        let recovery_filename = "silver_horizon.vol00+03.par2";
        let slice_size = 65_536;
        let described: Vec<(&str, &[u8])> = parts
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let par2_bytes = build_test_par2_index_for_files(&described, slice_size);
        let recovery_bytes = vec![0xAA; 64];

        // The NZB carries every part but one.
        let posted: Vec<(String, Vec<u8>)> = parts
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != withheld)
            .map(|(_, part)| part.clone())
            .collect();
        let mut files = posted.clone();
        files.push((index_filename.to_string(), par2_bytes.clone()));
        files.push((recovery_filename.to_string(), recovery_bytes.clone()));
        let working_dir = insert_active_job(
            &mut pipeline,
            job_id,
            rar_job_spec("Silver Horizon Withheld Part", &files),
        )
        .await;

        for (file_index, (filename, bytes)) in posted.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes)
                .await;
        }
        let index_file = posted.len() as u32;
        let recovery_file = index_file + 1;
        write_and_complete_file(
            &mut pipeline,
            job_id,
            index_file,
            index_filename,
            &par2_bytes,
        )
        .await;
        write_and_complete_file(
            &mut pipeline,
            job_id,
            recovery_file,
            recovery_filename,
            &recovery_bytes,
        )
        .await;
        // A full part spans three 64 KiB slices, so three recovery blocks
        // cover whichever part is withheld.
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set_for_files(&described, slice_size, 3),
            &[
                (index_file, index_filename, 0, false),
                (recovery_file, recovery_filename, 3, true),
            ],
        );
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }

        assert!(
            pipeline.job_has_sevenz_set_waiting_for_absent_volumes(job_id),
            "withheld part {withheld} should read as an absent 7z volume"
        );
        assert!(!working_dir.join(&withheld_name).exists());

        pipeline.check_job_completion(job_id).await;
        // The damaged-path read runs on a blocking worker; the repair it
        // decides is only reached once that verdict lands.
        settle_par2_analysis_work(&mut pipeline).await;

        assert_eq!(
            pipeline.par2_repairer_execute_calls, 1,
            "the absent part routes the job to a PAR2 repair"
        );
        assert_eq!(
            std::fs::read(working_dir.join(&withheld_name)).unwrap(),
            withheld_bytes,
            "the repair rebuilt the withheld part byte for byte"
        );
        {
            let state = pipeline.jobs.get(&job_id).unwrap();
            let topology = state
                .assembly
                .archive_topology_for(set_name)
                .expect("7z topology");
            assert_eq!(
                topology.volume_map.get(&withheld_name).copied(),
                Some(withheld as u32),
                "the rebuilt part is adopted into the topology"
            );
            assert_eq!(topology.expected_volume_count, Some(7));
            assert!(topology.complete_volumes.contains(&(withheld as u32)));
        }
        assert!(
            pipeline
                .archive_set_part_paths(job_id, set_name)
                .unwrap()
                .iter()
                .any(|path| path.ends_with(&withheld_name)),
            "the extractor is handed the rebuilt part"
        );
        assert!(!pipeline.job_has_sevenz_set_waiting_for_absent_volumes(job_id));

        // The repair tail spawns the extraction itself; a queued completion
        // check is the other road to the same place.
        for _ in 0..3 {
            if matches!(
                job_status_for_assert(&pipeline, job_id),
                Some(JobStatus::Extracting | JobStatus::QueuedExtract)
            ) {
                break;
            }
            let queued = pipeline
                .pending_completion_checks
                .pop_front()
                .expect("a completion check should be queued after repair");
            pipeline.check_job_completion(queued).await;
        }
        match next_extraction_done(&mut pipeline).await {
            ExtractionDone::FullSet {
                set_name: done_set,
                result,
                ..
            } => {
                assert_eq!(done_set, set_name);
                let outcome = result.expect("the whole set extracts");
                assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
                assert!(!outcome.extracted.is_empty());
            }
            _ => panic!("expected full-set extraction after the 7z part was rebuilt"),
        }
    }
}

/// The live ordering of a small posting: every data part and the index land
/// while the recovery file is still parked, so the completion pass runs with
/// the download pipeline not yet quiet. The absent part is structural — the
/// NZB never carried it — so the pass must not settle the set as clean and
/// hand a truncated archive to the extractor; it has to reach for the
/// recovery blocks. Then, once the recovery file lands, the part is rebuilt.
#[tokio::test]
async fn a_split_7z_short_of_a_part_the_nzb_never_carried_is_not_settled_clean_before_its_recovery_lands()
 {
    for (job_id, withheld) in [(JobId(30087), 2usize), (JobId(30088), 6usize)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let set_name = "generated_split_store_plain.7z";
        let parts = sevenz_fixture_bytes(set_name);
        let (withheld_name, withheld_bytes) = parts[withheld].clone();
        let index_filename = "silver_horizon.par2";
        let recovery_filename = "silver_horizon.vol00+03.par2";
        let slice_size = 65_536;
        let described: Vec<(&str, &[u8])> = parts
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let par2_bytes = build_test_par2_index_for_files(&described, slice_size);
        let recovery_bytes = vec![0xAA; 64];

        let posted: Vec<(String, Vec<u8>)> = parts
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != withheld)
            .map(|(_, part)| part.clone())
            .collect();
        let mut files = posted.clone();
        files.push((index_filename.to_string(), par2_bytes.clone()));
        files.push((recovery_filename.to_string(), recovery_bytes.clone()));
        let working_dir = insert_active_job(
            &mut pipeline,
            job_id,
            rar_job_spec("Silver Horizon Parked Recovery", &files),
        )
        .await;

        for (file_index, (filename, bytes)) in posted.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes)
                .await;
        }
        let index_file = posted.len() as u32;
        let recovery_file = index_file + 1;
        write_and_complete_file(
            &mut pipeline,
            job_id,
            index_file,
            index_filename,
            &par2_bytes,
        )
        .await;
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set_for_files(&described, slice_size, 3),
            &[
                (index_file, index_filename, 0, false),
                (recovery_file, recovery_filename, 3, false),
            ],
        );
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
            state.recovery_queue.push(DownloadWork {
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: recovery_file,
                    },
                    segment_number: 0,
                },
                message_id: MessageId::new("parked-7z-recovery@example.com"),
                groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
                priority: 1000,
                byte_estimate: 64,
                retry_count: 0,
                is_recovery: true,
                completion_critical: false,
                exclude_servers: Vec::new(),
                avoid_server: None,
            });
        }
        // The pipeline is not quiet: a decode is still in flight.
        pipeline.active_decodes_by_job.insert(job_id, 1);

        assert!(
            pipeline.job_has_sevenz_set_waiting_for_absent_volumes(job_id),
            "withheld part {withheld} is a structural absence, quiet or not"
        );

        pipeline.check_job_completion(job_id).await;

        assert!(
            !pipeline.par2_verified.contains(&job_id),
            "withheld part {withheld}: the set must not be settled clean; {}",
            debug_job_state(&pipeline, job_id)
        );
        assert!(
            !matches!(
                job_status_for_assert(&pipeline, job_id),
                Some(JobStatus::Failed { .. } | JobStatus::Complete)
            ),
            "withheld part {withheld}: {}",
            debug_job_state(&pipeline, job_id)
        );
        assert!(
            !working_dir.join(&withheld_name).exists(),
            "nothing can rebuild the part before the recovery file lands"
        );
        assert_eq!(pipeline.par2_repairer_execute_calls, 0);

        // The recovery file lands and the pipeline goes quiet.
        pipeline.active_decodes_by_job.remove(&job_id);
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }
        write_and_complete_file(
            &mut pipeline,
            job_id,
            recovery_file,
            recovery_filename,
            &recovery_bytes,
        )
        .await;
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set_for_files(&described, slice_size, 3),
            &[
                (index_file, index_filename, 0, false),
                (recovery_file, recovery_filename, 3, true),
            ],
        );
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }

        pipeline.check_job_completion(job_id).await;
        // The damaged-path read runs on a blocking worker; the repair it
        // decides is only reached once that verdict lands.
        settle_par2_analysis_work(&mut pipeline).await;

        assert_eq!(
            pipeline.par2_repairer_execute_calls,
            1,
            "withheld part {withheld}: the absent part routes the job to a PAR2 repair; {}",
            debug_job_state(&pipeline, job_id)
        );
        assert_eq!(
            std::fs::read(working_dir.join(&withheld_name)).unwrap(),
            withheld_bytes,
            "the repair rebuilt the withheld part byte for byte"
        );
        assert!(!pipeline.job_has_sevenz_set_waiting_for_absent_volumes(job_id));
    }
}

/// Two file members that sanitize to one destination are refused at open, and
/// that refusal has to end the job — not send it round again.
///
/// The refusal is raised by `ensure_unique_sanitized_rar_member_paths` before
/// any member is extracted, so the batch worker's error is the archive's
/// structure, not a member's bytes. Nothing about a retry can change it: no
/// re-download, no PAR2 verdict, no header refresh. Left classified as an
/// ordinary member failure, a job with no PAR2 set re-scheduled the same
/// members straight back into the same open — `set_failed_extraction_member`
/// only latches a member out while PAR2 still owes a verdict — and spun
/// extract/refuse/re-schedule as fast as the worker pool allowed.
#[tokio::test]
async fn colliding_member_paths_fail_the_job_instead_of_respinning_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30086);
    let files = build_case_colliding_multivolume_rar_set();
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Colliding Members", &files),
    )
    .await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 6).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a colliding archive must fail the job\n{}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("colliding sanitized member path"),
        "the failure names the collision: {error}"
    );
    // The first name reaches the disk before the second is even visible:
    // it becomes extractable once two volumes are down, and an archive opened
    // at that point has one started member and nothing to collide with. The
    // second name is what the open refuses, every time.
    assert!(
        pipeline
            .extracted_members
            .get(&job_id)
            .is_none_or(|members| !members.contains("CERTIFICATE/backup/id.bdmv")),
        "the refused member must never have been extracted"
    );
}

/// The terminal classification is keyed on the exact prefixes the open-time
/// checks produce. Anything a retry or a repair could still turn around —
/// a member CRC, a stale topology, FD pressure, a member whose *name* merely
/// contains one of the phrases — stays on its existing path.
#[test]
fn terminal_archive_structure_classification_matches_only_open_time_refusals() {
    assert!(Pipeline::is_terminal_archive_structure_error(
        "RAR archive contains colliding sanitized member path: CERTIFICATE/backup/id.bdmv"
    ));
    assert!(Pipeline::is_terminal_archive_structure_error(
        "unsafe RAR member path: ../escape.mkv"
    ));

    for error in [
        "checksum mismatch for member show.mkv",
        "crc mismatch extracting show.mkv",
        "member not found in archive: show.mkv",
        "RAR volume show.part02.rar unavailable",
        "no on-disk rar volumes for set show",
        "Too many open files (os error 24)",
        "failed to extract member colliding sanitized member path.mkv",
        "failed to extract member unsafe RAR member path.mkv",
        "",
    ] {
        assert!(
            !Pipeline::is_terminal_archive_structure_error(error),
            "must stay non-terminal: {error:?}"
        );
    }
}
