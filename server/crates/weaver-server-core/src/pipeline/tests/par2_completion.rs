use super::*;

#[tokio::test]
async fn restore_job_reloads_par2_metadata_from_disk_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let par2_filename = "repair.par2";
    let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
    let spec = par2_only_job_spec("PAR2 Restore", par2_filename, par2_bytes.len() as u32);
    let job_id = JobId(30030);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(par2_filename), &par2_bytes)
            .await
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

    assert!(restored.par2_set(job_id).is_some());
    let par2_set = restored.par2_set(job_id).unwrap();
    assert_eq!(par2_set.files.len(), 1);
    assert_eq!(par2_set.recovery_block_count(), 0);
}

#[tokio::test]
async fn par2_metadata_sanitizes_unsafe_canonical_target_before_rename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30151);
    let unsafe_canonical_filename = "Fixture.Payload.part001.rar\"";
    let sanitized_canonical_filename = "Fixture.Payload.part001.rar_";
    let obfuscated_filename = "51273aad56a8b904e96928935278a627.101";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = JobSpec {
        name: "PAR2 Sanitized Canonical Rebind".to_string(),
        password: None,
        total_bytes: rar_bytes.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: obfuscated_filename.to_string(),
            role: FileRole::from_filename(obfuscated_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: rar_bytes.len() as u32,
                message_id: "rar-sanitized-canonical@example.com".to_string(),
            }],
        }],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(unsafe_canonical_filename.to_string(), rar_bytes.clone())]),
        &[],
    );

    write_and_complete_file(&mut pipeline, job_id, 0, obfuscated_filename, &rar_bytes).await;
    pipeline.retry_par2_authoritative_identity(job_id).await;
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
        .expect("PAR2 should bind identity from sanitized canonical filename");
    assert_eq!(identity.current_filename, sanitized_canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(sanitized_canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(!working_dir.join(obfuscated_filename).exists());
    assert!(!working_dir.join(unsafe_canonical_filename).exists());
    assert!(working_dir.join(sanitized_canonical_filename).exists());

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("Fixture.Payload"))
        .cloned()
        .expect("sanitized PAR2 rebinding should rebuild RAR topology");
    assert!(
        topology
            .volume_map
            .contains_key(sanitized_canonical_filename)
    );
    assert!(!topology.volume_map.contains_key(unsafe_canonical_filename));
}

#[tokio::test]
async fn par2_metadata_records_canonical_name_without_phantom_current_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30113);
    let canonical_filename = "show.part001.rar";
    let source_filename = "incoming.part001.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = JobSpec {
        name: "PAR2 Canonical Before File Completion".to_string(),
        password: None,
        total_bytes: rar_bytes.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: source_filename.to_string(),
            role: FileRole::from_filename(source_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: rar_bytes.len() as u32,
                message_id: "rar-before-complete@example.com".to_string(),
            }],
        }],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes.clone())]),
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
        .expect("PAR2 should still bind identity by RAR volume number");
    assert_eq!(identity.current_filename, source_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(!working_dir.join(canonical_filename).exists());

    write_and_complete_file(&mut pipeline, job_id, 0, source_filename, &rar_bytes).await;
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
        .expect("data file identity should remain persisted");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert!(!working_dir.join(source_filename).exists());
    assert!(working_dir.join(canonical_filename).exists());
}

#[tokio::test]
async fn yenc_source_name_is_treated_as_expected_after_par2_rebind() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30128);
    let canonical_filename = "show.part001.rar";
    let source_filename = "incoming.part001.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = JobSpec {
        name: "PAR2 Rebind Preserves Source Filename".to_string(),
        password: None,
        total_bytes: rar_bytes.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: source_filename.to_string(),
            role: FileRole::from_filename(source_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: rar_bytes.len() as u32,
                message_id: "rar-source-name@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes.clone())]),
        &[],
    );

    write_and_complete_file(&mut pipeline, job_id, 0, source_filename, &rar_bytes).await;
    pipeline.retry_par2_authoritative_identity(job_id).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert!(pipeline.yenc_name_matches_rewritten_source(
        job_id,
        file_id,
        source_filename,
        canonical_filename,
    ));
}

#[tokio::test]
async fn par2_set_name_rebind_keeps_encrypted_multivolume_member_span_ready() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30133);
    let source_set_name = "incoming";
    let canonical_set_name = "video";
    let member_name = "test_clip.mkv";
    let canonical_files = vec![
        (
            "video.part001.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part1.rar"),
        ),
        (
            "video.part002.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part2.rar"),
        ),
        (
            "video.part003.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part3.rar"),
        ),
        (
            "video.part004.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part4.rar"),
        ),
        (
            "video.part005.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part5.rar"),
        ),
    ];
    let source_files: Vec<(String, Vec<u8>)> = canonical_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("incoming.part{:03}.rar", index + 1), bytes.clone()))
        .collect();
    let mut spec = rar_job_spec("PAR2 Rebind Encrypted Boundary", &source_files);
    spec.password = Some("testpass123".to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&canonical_files),
        &[],
    );

    for (index, (source_filename, bytes)) in source_files.iter().enumerate().take(4) {
        write_and_complete_rar_volume(&mut pipeline, job_id, index as u32, source_filename, bytes)
            .await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, canonical_set_name)
        .expect("canonical encrypted snapshot should exist after four rebound volumes");
    let mut cached = serde_json::to_value(
        rmp_serde::from_slice::<weaver_unrar::CachedArchiveHeaders>(&cached_headers).unwrap(),
    )
    .unwrap();
    let clip = cached["members"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|member| member["name"] == member_name)
        .expect("cached snapshot should contain the encrypted clip member");
    let first_segment = clip["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("encrypted clip should keep its first segment");
    clip["segments"] = serde_json::json!([first_segment]);
    clip["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    pipeline
        .rar_sets
        .get_mut(&(job_id, canonical_set_name.to_string()))
        .expect("canonical set should exist after PAR2 rebind")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, canonical_set_name, &stale_headers)
        .unwrap();

    write_and_complete_rar_volume(
        &mut pipeline,
        job_id,
        4,
        &source_files[4].0,
        &source_files[4].1,
    )
    .await;
    pipeline.retry_par2_authoritative_identity(job_id).await;
    drain_rar_refreshes(&mut pipeline).await;

    assert_eq!(
        member_span(&pipeline, job_id, canonical_set_name, member_name),
        Some((0, 4))
    );
    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, canonical_set_name);
    let selected = pipeline.volume_paths_for_rar_members(
        job_id,
        canonical_set_name,
        &[member_name.to_string()],
        &volume_paths,
        true,
        false,
    );
    assert_eq!(
        selected.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, source_set_name.to_string())),
        "the pre-rebind RAR set should not survive after canonical migration"
    );
}

#[tokio::test]
async fn clean_par2_quick_verification_completes_direct_payload_without_authoritative_verify() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30114);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..32u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Clean Direct Payload Quick Verify",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;

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

    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

#[tokio::test]
async fn clean_par2_quick_verification_exits_verifying_for_split_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30115);
    let files = vec![
        ("archive.001".to_string(), b"hello ".to_vec()),
        ("archive.002".to_string(), b"world".to_vec()),
    ];
    let spec = rar_job_spec("Clean PAR2 Split Verify Starts Join", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
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
            assert_eq!(set_name, "archive");
            assert!(result.is_ok());
        }
        _ => panic!("expected split join extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn corrupt_single_sevenz_enters_authoritative_par2_verification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30127);
    let archive_filename = "archive.7z";
    let original_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let mut damaged_bytes = original_bytes.clone();
    damaged_bytes[7] ^= 0xFF;
    let spec = standalone_job_spec(
        "Corrupt PAR2 Single 7z Requires Verify",
        &[(archive_filename.to_string(), damaged_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), original_bytes)]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &damaged_bytes).await;
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

    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(!pipeline.inflight_extractions.contains_key(&job_id));
}

#[tokio::test]
async fn restore_job_reparses_par2_without_promoted_recovery_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
    let spec = JobSpec {
        name: "PAR2 Promote Restore".to_string(),
        password: None,
        total_bytes: par2_bytes.len() as u64 + 64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "par2-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "par2-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let job_id = JobId(30032);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(index_filename), &par2_bytes)
            .await
            .unwrap();
        pipeline
            .db
            .upsert_par2_file(job_id, 1, recovery_filename, 1, true)
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
            complete_files: HashSet::from([NzbFileId {
                job_id,
                file_index: 0,
            }]),
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

    assert!(restored.par2_set(job_id).is_some());
    assert_eq!(
        restored
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&1))
            .map(|file| (file.recovery_blocks, file.promoted)),
        None
    );

    let state = restored.jobs.get_mut(&job_id).unwrap();
    let mut queued = state.download_queue.drain_all();
    queued.sort_by_key(|work| work.segment_id.file_id.file_index);
    assert!(queued.is_empty());
    assert!(state.recovery_queue.has_recovery_work());
}

#[tokio::test]
async fn no_par2_full_set_failure_requeues_archive_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30026);
    let spec = JobSpec {
        name: "No PAR2 ZIP Retry".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "archive.zip".to_string(),
            role: FileRole::from_filename("archive.zip"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "zip-0@example.com".to_string(),
            }],
        }],
    };
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
            .commit_segment(0, 128)
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
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["archive.zip".to_string()]));

    pipeline.check_job_completion(job_id).await;

    let complete_data_files = {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        assert!(matches!(state.status, JobStatus::Downloading));
        assert_eq!(state.download_queue.len(), 1);
        let queued = state
            .download_queue
            .pop()
            .expect("archive source should be requeued");
        assert!(queued.exclude_servers.is_empty());
        state.assembly.complete_data_file_count()
    };
    assert_eq!(complete_data_files, 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn par2_verified_complete_archive_refreshes_missing_existing_topology_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30030);
    let filename = "archive.7z";
    let spec = JobSpec {
        name: "PAR2 Existing Complete Archive Refresh".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "par2-existing-complete-archive@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
    }
    let verification = weaver_par2::VerificationResult {
        files: vec![weaver_par2::verify::FileVerification {
            file_id: weaver_par2::FileId::from_bytes([0u8; 16]),
            filename: filename.to_string(),
            status: weaver_par2::verify::FileStatus::Complete,
            valid_slices: Vec::new(),
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: weaver_par2::verify::Repairability::NotNeeded,
    };

    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(job_id, &verification),
        vec![file_id],
        "already-complete PAR2-verified archives without topology must be refreshed"
    );

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: Vec::new(),
                unresolved_spans: Vec::new(),
            },
        );

    assert!(
        pipeline
            .verified_complete_archive_file_ids_needing_refresh(job_id, &verification)
            .is_empty(),
        "existing topology should avoid a redundant refresh for unchanged complete files"
    );
}

#[tokio::test]
async fn direct_payload_par2_copy_only_repair_does_not_require_recovery_blocks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30086);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let spec = JobSpec {
        name: "Direct Payload PAR2 Copy Only Repair".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len()) as u64,
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
                        bytes: 64,
                        message_id: "copy-only-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "copy-only-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "copy-only-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    tokio::fs::write(
        working_dir.join("payload-second-block.bin"),
        &original_payload[64..128],
    )
    .await
    .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
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
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[(1, index_filename, 0, false)],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Direct Payload PAR2 Copy Only Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[tokio::test]
async fn direct_payload_par2_repair_verifies_complete_corrupt_payload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut verify_events = pipeline.event_tx.subscribe();
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30087);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Complete Direct Payload PAR2 Repair".to_string(),
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
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "complete-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "complete-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "complete-payload-index@example.com".to_string(),
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
                    message_id: "complete-payload-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        1
    );
    assert_eq!(drain_job_repair_complete(&mut repair_events, job_id), 1);
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Complete Direct Payload PAR2 Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[tokio::test]
async fn restored_repairing_payload_uses_single_repairer_analyze_and_execute_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut verify_events = pipeline.event_tx.subscribe();
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30187);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Restored Repairing Direct Payload PAR2 Repair".to_string(),
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
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "restored-complete-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "restored-complete-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "restored-complete-payload-index@example.com".to_string(),
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
                    message_id: "restored-complete-payload-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Repairing;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        1
    );
    assert_eq!(drain_job_repair_complete(&mut repair_events, job_id), 1);
    assert_eq!(pipeline.par2_lower_bound_preflight_calls, 0);
    assert_eq!(pipeline.par2_authoritative_verify_calls, 0);
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);

    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Restored Repairing Direct Payload PAR2 Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[tokio::test]
async fn complete_payload_does_not_finalize_while_promoted_recovery_is_pending() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30088);
    let payload_filename = "payload.mkv";
    let payload = vec![0x42; 128];
    let spec = segmented_job_spec(
        "Pending Recovery Completion Guard",
        payload_filename,
        &[128],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("promoted-recovery@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.jobs.contains_key(&job_id));
    assert!(
        !pipeline
            .complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(
                "Pending Recovery Completion Guard",
            ))
            .exists()
    );
}

#[tokio::test]
async fn complete_payload_finalizes_while_optional_recovery_is_parked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let payload_filename = "payload.mkv";
    let payload = vec![0x42; 128];
    let spec = segmented_job_spec(
        "Optional Recovery Completion Guard",
        payload_filename,
        &[128],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("optional-recovery-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Optional Recovery Completion Guard",
    ));
    assert!(output_dir.join(payload_filename).exists());
}

#[tokio::test]
async fn complete_direct_payload_with_loaded_par2_does_not_finalize_with_parked_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let mut verify_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30093);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let spec = JobSpec {
        name: "Loaded PAR2 Parked Recovery Guard".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + 64) as u64,
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
                        bytes: 64,
                        message_id: "loaded-par2-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "loaded-par2-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "loaded-par2-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "loaded-par2-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let payload_file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("loaded-par2-recovery@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, false),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        1
    );
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.download_queue.has_recovery_work() || state.recovery_queue.has_recovery_work());
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(
                "Loaded PAR2 Parked Recovery Guard",
            ))
            .exists()
    );
}

#[tokio::test]
async fn archive_payload_does_not_extract_while_promoted_recovery_is_pending() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let archive_filename = "archive.7z";
    let recovery_filename = "archive.7z.vol00+01.par2";
    let spec = JobSpec {
        name: "Pending Recovery Archive Extraction Guard".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: archive_filename.to_string(),
                role: FileRole::from_filename(archive_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "pending-recovery-archive@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "pending-recovery-archive-volume@example.com".to_string(),
                }],
            },
        ],
    };
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
            .commit_segment(0, 128)
            .unwrap();
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
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("pending-recovery-archive-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(!pipeline.inflight_extractions.contains_key(&job_id));
}

#[tokio::test]
async fn cancel_job_clears_promoted_recovery_runtime_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    let recovery_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let spec = JobSpec {
        name: "Cancel Promoted Recovery Runtime".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::from_filename("payload.bin"),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "cancel-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.vol00+01.par2".to_string(),
                role: FileRole::from_filename("payload.vol00+01.par2"),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "cancel-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: recovery_file_id,
                segment_number: 0,
            },
            message_id: MessageId::new("cancel-promoted-queued@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: recovery_file_id,
                segment_number: 1,
            },
            message_id: MessageId::new("cancel-promoted-parked@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(recovery_file_id, 1);
    pipeline.active_decodes_by_job.insert(job_id, 1);
    pipeline.active_decodes_by_file.insert(recovery_file_id, 1);
    pipeline.pending_retries_by_job.insert(job_id, 1);
    pipeline.pending_retries_by_segment.insert(
        SegmentId {
            file_id: recovery_file_id,
            segment_number: 2,
        },
        1,
    );
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(SegmentId {
            file_id: recovery_file_id,
            segment_number: 3,
        });
    pipeline.schedule_job_completion_check(job_id);

    let (reply, result) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJob { job_id, reply })
        .await;
    result.await.unwrap().unwrap();

    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.job_order.contains(&job_id));
    assert!(pipeline.par2_runtime(job_id).is_none());
    assert!(!pipeline.active_downloads_by_job.contains_key(&job_id));
    assert!(
        !pipeline
            .active_downloads_by_file
            .contains_key(&recovery_file_id)
    );
    assert!(!pipeline.active_decodes_by_job.contains_key(&job_id));
    assert!(
        !pipeline
            .active_decodes_by_file
            .contains_key(&recovery_file_id)
    );
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(
        !pipeline
            .pending_retries_by_segment
            .keys()
            .any(|segment_id| segment_id.file_id.job_id == job_id)
    );
    assert!(
        !pipeline
            .unavailable_promoted_recovery_segments
            .iter()
            .any(|segment_id| segment_id.file_id.job_id == job_id)
    );
    assert!(!pipeline.pending_completion_checks.contains(&job_id));
}

#[tokio::test]
async fn promoted_recovery_wait_does_not_reverify_until_recovery_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30089);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let spec = JobSpec {
        name: "Pending Targeted Recovery Verify Guard".to_string(),
        password: None,
        total_bytes: original_payload.len() as u64 + par2_bytes.len() as u64 + 64,
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
                        bytes: 64,
                        message_id: "pending-recovery-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "pending-recovery-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "pending-recovery-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "pending-recovery-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &damaged_payload).await;
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("pending-recovery-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, false),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.download_queue.has_recovery_work())
    );

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );

    let queued_recovery = {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.drain_all()
    };
    assert_eq!(queued_recovery.len(), 1);
    assert_eq!(
        queued_recovery[0].segment_id.file_id.file_index, 2,
        "promoted recovery should be the only queued work"
    );

    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &[0xAA; 64]).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
}

#[tokio::test]
async fn promoted_recovery_retry_reenters_dispatchable_queue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30093);
    let recovery_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let segment_id = SegmentId {
        file_id: recovery_file_id,
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Promoted Recovery Retry Routing".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::from_filename("payload.bin"),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "retry-routing-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload.vol00+01.par2".to_string(),
                role: FileRole::from_filename("payload.vol00+01.par2"),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "retry-routing-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;

    pipeline.note_retry_scheduled(segment_id);
    pipeline.requeue_retry_work(DownloadWork {
        segment_id,
        message_id: MessageId::new("retry-routing-recovery@example.com"),
        groups: vec!["alt.binaries.test".to_string()],
        priority: 1000,
        byte_estimate: 64,
        retry_count: 1,
        is_recovery: true,
        exclude_servers: Vec::new(),
    });

    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(
        !pipeline
            .pending_retries_by_segment
            .contains_key(&segment_id)
    );
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    assert!(state.recovery_queue.is_empty());
    let queued = state
        .download_queue
        .pop()
        .expect("promoted recovery retry should be dispatchable");
    assert_eq!(queued.segment_id, segment_id);
    assert_eq!(queued.priority, super::repair::PROMOTED_RECOVERY_PRIORITY);
}

#[tokio::test]
async fn unavailable_promoted_recovery_promotes_next_candidate_before_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30094);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let first_recovery_filename = "repair.vol00+01.par2";
    let second_recovery_filename = "repair.vol01+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let first_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };
    let second_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 3,
        },
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Promoted Recovery Candidate Fallback".to_string(),
        password: None,
        total_bytes: original_payload.len() as u64 + par2_bytes.len() as u64 + 96,
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
                        bytes: 64,
                        message_id: "fallback-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "fallback-payload-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "fallback-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: first_recovery_filename.to_string(),
                role: FileRole::from_filename(first_recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "fallback-recovery-small@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: second_recovery_filename.to_string(),
                role: FileRole::from_filename(second_recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "fallback-recovery-large@example.com".to_string(),
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
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state.recovery_queue.push(DownloadWork {
            segment_id: first_segment,
            message_id: MessageId::new("fallback-recovery-small@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 32,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
        state.recovery_queue.push(DownloadWork {
            segment_id: second_segment,
            message_id: MessageId::new("fallback-recovery-large@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, first_recovery_filename, 1, false),
            (3, second_recovery_filename, 1, false),
        ],
    );

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.is_promoted_recovery_file(job_id, 2));
    assert!(!pipeline.is_promoted_recovery_file(job_id, 3));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].segment_id, first_segment);
    }

    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.is_promoted_recovery_file(job_id, 3));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].segment_id, second_segment);
    }

    pipeline.mark_promoted_recovery_segment_unavailable(second_segment);
    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should fail only after promoted recovery candidates are exhausted");
    };
    assert!(error.contains("only 0 recovery blocks available in NZB"));
}

#[tokio::test]
async fn active_recovery_from_another_job_does_not_satisfy_promoted_recovery_wait() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let waiting_job_id = JobId(30095);
    let other_job_id = JobId(30096);
    let spec = segmented_job_spec("Promoted Recovery Isolation", "payload.bin", &[128]);
    insert_active_job(&mut pipeline, waiting_job_id, spec.clone()).await;
    insert_active_job(&mut pipeline, other_job_id, spec).await;
    pipeline
        .ensure_par2_runtime(waiting_job_id)
        .files
        .entry(1)
        .or_default()
        .promoted = true;

    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id: other_job_id,
            file_index: 1,
        },
        1,
    );
    assert!(!pipeline.promoted_recovery_file_has_pending_work(waiting_job_id, 1));

    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id: waiting_job_id,
            file_index: 1,
        },
        1,
    );
    assert!(pipeline.promoted_recovery_file_has_pending_work(waiting_job_id, 1));
}

#[tokio::test]
async fn direct_payload_par2_repair_fails_when_recovery_is_insufficient() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30081);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| ((value * 7) % 251) as u8).collect();
    let damaged_payload = vec![0u8; original_payload.len()];
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0x55; 64];
    let spec = JobSpec {
        name: "Direct Payload PAR2 Failure".to_string(),
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
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "payload-fail-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "payload-fail-1@example.com".to_string(),
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
                    bytes: par2_bytes.len() as u32,
                    message_id: "payload-fail-index@example.com".to_string(),
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
                    message_id: "payload-fail-recovery@example.com".to_string(),
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
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.note_released_download_result_pending(job_id, 512);
    pipeline.check_job_completion(job_id).await;

    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "pending released download results must defer PAR2 fail-fast"
    );

    pipeline.finish_released_download_result_processing(job_id, 512);
    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed when recovery blocks are insufficient");
    };
    assert!(error.contains("not repairable"));
}

#[tokio::test]
async fn extracted_archive_job_finalizes_without_reverifying_missing_par2_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30084);
    let files = build_multifile_multivolume_rar_set();
    let mut spec = rar_job_spec("RAR Finalize Skips Missing PAR2 Index", &files);
    spec.total_bytes += 128;
    spec.files.push(FileSpec {
        filename: "repair.par2".to_string(),
        role: FileRole::from_filename("repair.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-index@example.com".to_string(),
        }],
    });
    spec.files.push(FileSpec {
        filename: "repair.vol00+01.par2".to_string(),
        role: FileRole::from_filename("repair.vol00+01.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-recovery@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
        let file_id = NzbFileId {
            job_id,
            file_index: file_index as u32,
        };
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state
                .assembly
                .file_mut(file_id)
                .unwrap()
                .commit_segment(0, bytes.len() as u32)
                .unwrap();
        }
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, false)
            .await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(&files[0].0, &files[0].1, 64, 0),
        &[],
    );

    let extraction_staging_dir = pipeline.extraction_staging_dir(job_id);
    for (member_name, bytes) in [
        ("E01.mkv", b"episode-a-payload".as_slice()),
        ("E02.mkv", b"episode-b-payload".as_slice()),
    ] {
        let (output_path, _) = Pipeline::member_output_paths(&extraction_staging_dir, member_name);
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&output_path, bytes).await.unwrap();
        pipeline
            .db
            .add_extracted_member(job_id, member_name, &output_path)
            .unwrap();
        pipeline
            .extracted_members
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
    }
    // This fixture models restart recovery after extraction already completed.
    // Do not leave the eager extraction workers spawned during setup in scope.
    for ((rar_job_id, _), set_state) in pipeline.rar_sets.iter_mut() {
        if *rar_job_id == job_id {
            set_state.active_workers = 0;
            set_state.in_flight_members.clear();
        }
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    for (filename, _) in &files {
        tokio::fs::remove_file(working_dir.join(filename))
            .await
            .unwrap();
    }

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR Finalize Skips Missing PAR2 Index",
    ));
    assert!(output_dir.join("E01.mkv").exists());
    assert!(output_dir.join("E02.mkv").exists());
}

#[test]
fn quick_par2_verification_uses_verifying_failpoint() {
    assert_eq!(
        Pipeline::par2_verification_started_failpoint_name(),
        crate::e2e_failpoint::STATUS_ENTER_VERIFYING
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Verifying,
            crate::jobs::model::RunState::Active,
        ),
        Some(Pipeline::par2_verification_started_failpoint_name())
    );
}

#[tokio::test]
async fn clean_verify_after_swap_correction_preserves_retry_frontier_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30017);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Retry Frontier", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let part03 = working_dir.join("show.part03.rar");
    let part04 = working_dir.join("show.part04.rar");
    let swap_tmp = working_dir.join("show.swap.tmp");
    tokio::fs::rename(&part03, &swap_tmp).await.unwrap();
    tokio::fs::rename(&part04, &part03).await.unwrap();
    tokio::fs::rename(&swap_tmp, &part04).await.unwrap();

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

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part01.rar".to_string(), "show.part02.rar".to_string()]
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
        .expect("RAR plan should still exist after normalization retry");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert!(!plan.waiting_on_volumes.contains(&0));
    assert!(plan.waiting_on_volumes.is_disjoint(&plan.deletion_eligible));
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv")
    );
    assert_eq!(
        plan.delete_decisions
            .get(&2)
            .expect("volume 2 decision should exist")
            .owners,
        vec!["E02.mkv".to_string()]
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
async fn health_probe_candidates_skip_par2_segments() {
    let spec = standalone_with_par2_job_spec("Probe Candidates", 128, 64);

    let probes = Pipeline::health_probe_candidates(&spec);

    assert_eq!(probes, vec!["payload@example.com".to_string()]);
}

#[tokio::test]
async fn probe_projection_uses_only_payload_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = JobSpec {
        name: "Probe Projection".to_string(),
        password: None,
        total_bytes: 592,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload-a.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "payload-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "payload-b@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 16,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 320,
                    message_id: "repair-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        total: 2,
        missed: 1,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.failed_bytes, 128);
    assert_eq!(state.last_health_probe_failed_bytes, 128);
    assert!(matches!(state.status, JobStatus::Downloading));
}

#[tokio::test]
async fn reconcile_job_progress_leaves_terminal_recovery_to_restore_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let spec = standalone_job_spec(
        "Restored Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.health_probing = false;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.download_state = crate::jobs::model::DownloadState::Checking;
        state.refresh_legacy_status();
    }

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Checking
    ));
    assert!(matches!(state.status, JobStatus::Checking));
}

#[tokio::test]
async fn health_below_critical_without_par2_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec("No PAR2 Health Fail", &[("payload.bin".to_string(), 100)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 20;
    }

    pipeline.check_health(job_id);

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn health_below_critical_with_par2_defers_to_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let spec = JobSpec {
        name: "PAR2 Health Defers".to_string(),
        password: None,
        total_bytes: 300,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload-a.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "payload-b.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-b@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 100,
                    message_id: "par2-health-repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 200;
    }

    pipeline.check_health(job_id);

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(pipeline.pending_completion_checks.contains(&job_id));
}

#[tokio::test]
async fn repair_queue_limits_to_one_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31001);
    let job_b = JobId(31002);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert_eq!(
        pipeline.jobs.get(&job_a).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );
}

#[tokio::test]
async fn restore_repairing_preserves_status_and_slot_ownership() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31205);
    let spec = standalone_job_spec("Restore repairing", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-repairing");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Repairing,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: Some(42_000.0),
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(42_000.0)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
}

#[tokio::test]
async fn repair_queue_promotion_reserves_slot_and_keeps_queue_age() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31211);
    let job_b = JobId(31212);
    let job_c = JobId(31213);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );
    pipeline.jobs.insert(
        job_c,
        minimal_job_state(job_c, "repair-c", temp_dir.path().join("repair-c")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );

    assert!(!pipeline.maybe_start_repair(job_c).await);
    assert_eq!(
        pipeline.jobs.get(&job_c).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn pause_rejects_queued_repair_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31221);
    let job_b = JobId(31222);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    let error = pipeline.pause_job_runtime(job_b).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );
}
