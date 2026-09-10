//! `par2_completion` tests, part of a mechanical split of the original file.

use super::*;

#[test]
fn bounded_repair_reconstructs_a_missing_payload() {
    let temp = tempfile::tempdir().unwrap();
    let payload = b"complete";
    let set = build_repairable_par2_set("payload.bin", payload, 4, 2);
    let outcome = run_file_descriptor_bounded_par2_repair(
        temp.path().to_path_buf(),
        set,
        HashMap::new(),
        Vec::new(),
        64 * 1024 * 1024,
        par2_rs::CancellationToken::new(),
        None,
    )
    .unwrap();
    assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Repaired);
    assert_eq!(
        std::fs::read(temp.path().join("payload.bin")).unwrap(),
        payload
    );
}

/// Admission, authenticated metadata ingestion, the normal repair worker, and
/// final output reconciliation must all tolerate a payload with no disk inode.
#[tokio::test]
async fn admitted_job_reconstructs_an_entirely_missing_payload() {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let payload = b"complete generated payload";
    std::fs::write(source.join("payload.bin"), payload).unwrap();
    let mut options = Par2CreatorOptions::with_output(
        source.join("repair.par2"),
        Some(source.clone()),
        vec![source.join("payload.bin")],
    );
    options.block_sizing = BlockSizing::Bytes(16);
    options.recovery_amount = RecoveryAmount::Count(2);
    options.volume_count = Some(1);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let carriers: Vec<_> = created
        .output_paths
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    let mut files = vec![("payload.bin".to_owned(), payload.len() as u32)];
    files.extend(
        carriers
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.len() as u32)),
    );
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(11738);
    let job_name = "Generated Missing Payload";
    pipeline
        .add_job(
            job_id,
            standalone_job_spec(job_name, &files),
            PathBuf::from("missing.nzb"),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    let working = pipeline.jobs[&job_id].working_dir.clone();
    assert!(!working.join("payload.bin").exists());
    // The transport supplies completed carriers and a terminal not-found result
    // for the payload. No repair set or source binding is injected by the test.
    for (index, (name, bytes)) in carriers.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32 + 1, name, bytes).await;
        pipeline
            .try_load_par2_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: index as u32 + 1,
                },
            )
            .await;
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            runtime_generation: pipeline.pool_generation,
            data: Err(DownloadError::from_nntp(
                weaver_nntp::NntpError::ArticleNotFound,
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: u32::MAX,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let drained = drain_job_events(&mut events, job_id);
    assert!(
        drained.iter().any(|event| matches!(
            event,
            PipelineEvent::RepairComplete {
                slices_repaired: 2,
                ..
            }
        )),
        "{drained:?}"
    );
    assert_eq!(
        std::fs::read(pipeline.complete_dir.join(job_name).join("payload.bin")).unwrap(),
        payload
    );
}

#[cfg(any(unix, windows))]
#[test]
fn par2_session_io_errors_preserve_file_descriptor_exhaustion() {
    #[cfg(unix)]
    let raw_os_error = libc::EMFILE;
    #[cfg(windows)]
    let raw_os_error = 4;

    let exhausted = par2_rs::Par2SessionError::Par2(par2_rs::Par2Error::Io(
        std::io::Error::from_raw_os_error(raw_os_error),
    ));
    assert!(error_chain_has_file_descriptor_exhaustion(&exhausted));

    let ordinary = par2_rs::Par2SessionError::Par2(par2_rs::Par2Error::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "missing source",
    )));
    assert!(!error_chain_has_file_descriptor_exhaustion(&ordinary));
}

#[test]
fn descriptor_bounded_repair_handles_a_large_multifile_set() {
    const FILE_COUNT: usize = 128;
    const SLICE_SIZE: u64 = 2;

    let temp_dir = tempfile::tempdir().unwrap();
    let files = (0..FILE_COUNT)
        .map(|index| {
            (
                format!("volume-{index:03}.rar"),
                vec![index as u8, index.wrapping_add(1) as u8],
            )
        })
        .collect::<Vec<_>>();
    let file_refs = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    let par2_set = build_repairable_par2_set_for_files(&file_refs, SLICE_SIZE, 1);
    let damaged_name = &files[FILE_COUNT - 1].0;
    let expected_repaired = files[FILE_COUNT - 1].1.clone();

    for (name, bytes) in &files {
        let contents = if name == damaged_name {
            vec![0, 0]
        } else {
            bytes.clone()
        };
        std::fs::write(temp_dir.path().join(name), contents).unwrap();
    }

    let evidence = par2_set
        .files
        .iter()
        .filter(|(_, description)| &description.filename != damaged_name)
        .map(|(file_id, description)| {
            let proof =
                par2_rs::InStreamCrc32Proof::try_new(description.length, true, true, true).unwrap();
            par2_rs::SliceEvidence::from_in_stream_crc32(
                par2_set.recovery_set_id,
                *file_id,
                0,
                true,
                proof,
            )
        })
        .collect::<Vec<_>>();

    let verification = par2_rs::VerificationResult {
        files: par2_set
            .recovery_file_ids
            .iter()
            .map(|file_id| {
                let damaged = par2_set.files[file_id].filename == *damaged_name;
                par2_rs::FileVerification {
                    file_id: *file_id,
                    filename: par2_set.files[file_id].filename.clone(),
                    status: if damaged {
                        par2_rs::FileStatus::Damaged(1)
                    } else {
                        par2_rs::FileStatus::Complete
                    },
                    valid_slices: vec![!damaged],
                    missing_slice_count: u32::from(damaged),
                }
            })
            .collect(),
        recovery_blocks_available: 1,
        total_missing_blocks: 1,
        repairable: par2_rs::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 1,
        },
    };
    assert!(bounded_repair_evidence_covers_assessment(
        &verification,
        &evidence
    ));
    assert!(!bounded_repair_evidence_covers_assessment(
        &verification,
        &evidence[..evidence.len() - 1]
    ));

    let outcome = run_file_descriptor_bounded_par2_repair(
        temp_dir.path().to_path_buf(),
        par2_set,
        HashMap::new(),
        evidence,
        64 * 1024 * 1024,
        par2_rs::CancellationToken::new(),
        None,
    )
    .unwrap();

    assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Repaired);
    assert_eq!(
        std::fs::read(temp_dir.path().join(damaged_name)).unwrap(),
        expected_repaired
    );
}

#[tokio::test]
async fn restore_job_reloads_par2_metadata_from_disk_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let par2_filename = "repair.par2";
    let payload = b"payload-data";
    let par2_bytes = build_test_par2_index("payload.bin", payload, 8);
    let mut spec = par2_only_job_spec("PAR2 Restore", par2_filename, par2_bytes.len() as u32);
    spec.total_bytes += payload.len() as u64;
    spec.files.push(FileSpec {
        filename: "payload.bin".to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: payload.len() as u32,
            message_id: "restored-payload@example.com".to_string(),
        }],
    });
    let job_id = JobId(30030);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(par2_filename), &par2_bytes)
            .await
            .unwrap();
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
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
    let par2_set = restored.par2_set(job_id).unwrap();
    assert_eq!(par2_set.files.len(), 1);
    assert_eq!(par2_set.recovery_block_count(), 0);
    assert!(
        !restored
            .par2_runtime(job_id)
            .unwrap()
            .explicit_index_bootstrap_closed,
        "restored runtime must recompute the lease gate from durable metadata"
    );
    assert!(matches!(
        restored.par2_discovery_state_for_candidate(job_id, 0),
        Par2DiscoveryState::Parsed { .. }
    ));

    let restored_state = restored.jobs.get(&job_id).unwrap();
    assert_eq!(
        restored_state.download_queue.len(),
        1,
        "restored payload queue must survive progress reconciliation (status {:?}, recovery {})",
        restored_state.status,
        restored_state.recovery_queue.len()
    );
    let pressure = restored.refresh_download_pressure();
    let payload_lease = restored
        .try_lease_initial_download_batch_for_test(job_id, pressure)
        .expect("restored parsed index must not leave payload blocked");
    assert!(matches!(
        payload_lease.checkpoint_plan,
        weaver_yenc::CheckpointPlan::Single(_)
    ));
    assert!(
        restored
            .par2_runtime(job_id)
            .unwrap()
            .explicit_index_bootstrap_closed
    );
}

#[tokio::test]
async fn restored_unknown_par2_is_inspected_on_completion_not_startup() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(30031);
    let par2_filename = "c71a5f0d";
    let payload_filename = "5d420be9";
    let payload = b"restored opaque payload";
    let par2 = build_test_par2_index("restored.mkv", payload, 8);
    let spec = JobSpec {
        name: "Restored Misnamed PAR2".to_string(),
        password: None,
        total_bytes: (par2.len() + payload.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: par2_filename.to_string(),
                role: FileRole::from_filename(par2_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2.len() as u32,
                    message_id: "restored-opaque-par2@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "restored-opaque-payload@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(par2_filename), &par2)
            .await
            .unwrap();
        tokio::fs::write(working_dir.join(payload_filename), payload)
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

    assert!(
        restored.par2_set(job_id).is_none(),
        "restoring does not scan arbitrary completed files"
    );
    restored.probe_restored_par2_headers(job_id).await;
    assert!(
        restored.par2_set(job_id).is_some(),
        "the normal completion path discovers a valid opaque PAR2 header"
    );
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
        rmp_serde::from_slice::<unrar_rs::CachedArchiveHeaders>(&cached_headers).unwrap(),
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
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
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
async fn damaged_in_stream_verdict_blocks_quick_verification_even_with_matching_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30177);
    let payload_filename = "payload.mkv";
    let expected: Vec<u8> = (0..32u32).map(|value| (value % 251) as u8).collect();
    let mut actual = expected.clone();
    actual[7] ^= 0xFF; // same length, different bytes on the wire
    let spec = standalone_job_spec(
        "Damaged Verdict Blocks Quick Verify",
        &[(payload_filename.to_string(), expected.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    // The recovery set describes `expected`, IFSC reference included: one
    // slice covering the whole file.
    let mut par2_set = placement_par2_file_set(&[(payload_filename.to_string(), expected.clone())]);
    let par2_file_id = par2_set.recovery_file_ids[0];
    par2_set.slice_checksums.insert(
        par2_file_id,
        vec![par2_rs::SliceChecksum {
            crc32: par2_rs::checksum::crc32(&expected),
            md5: par2_rs::checksum::md5(&expected),
        }],
    );
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    // What actually arrived is `actual` ...
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &actual).await;
    // ... while the trusted store claims a digest equal to the PAR2
    // expectation — the poisoned shape the removed expected-hash substitution
    // used to produce, indistinguishable from a stale trusted row whose bytes
    // were rewritten after it was recorded. On hash comparison alone, quick
    // verification would pass tautologically.
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &expected).await;

    // The dual-CRC grid saw the real bytes: one pCRC-verified article covers
    // the whole file and its derived slice CRC contradicts the IFSC reference.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let wire_crc = par2_rs::checksum::crc32(&actual);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        actual.len() as u64,
        wire_crc,
        true,
        false,
        &[weaver_yenc::Segment {
            file_offset: 0,
            len: actual.len() as u64,
            crc32: wire_crc,
        }],
    );
    assert!(
        pipeline
            .block_crc_verdicts(file_id)
            .is_some_and(|verdicts| {
                verdicts.values().any(|verdict| {
                    matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)
                })
            }),
        "precondition: the grid must call this file Damaged"
    );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    // The matching digest must not override the in-stream verdict: quick
    // verification refuses and the authoritative pass owns the file.
    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn metadata_early_clean_download_quick_completes_from_the_dual_crc_grid_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30178);
    let payload_filename = "payload.mkv";
    let unrelated_par2_filename = "00-unrelated.par2";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    let unrelated_par2 = b"unrelated completed PAR2 metadata".to_vec();
    let spec = standalone_job_spec(
        "Metadata Early Clean Grid Quick Verify",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            (
                unrelated_par2_filename.to_string(),
                unrelated_par2.len() as u32,
            ),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Metadata-early: the recovery set (two 32-byte slices, IFSC included) is
    // installed before any article lands, which is exactly the shape that
    // streams no MD5 at all.
    let mut par2_set = placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]);
    par2_set.slice_size = 32;
    let par2_file_id = par2_set.recovery_file_ids[0];
    let slice_checksums: Vec<par2_rs::SliceChecksum> = payload
        .chunks(32)
        .map(|slice| {
            let mut state = par2_rs::SliceChecksumState::new();
            state.update(slice);
            let (crc32, md5) = state.finalize(Some(32));
            par2_rs::SliceChecksum { crc32, md5 }
        })
        .collect();
    par2_set
        .slice_checksums
        .insert(par2_file_id, slice_checksums);
    assert_ne!(
        par2_rs::checksum::md5(&unrelated_par2),
        par2_set.file_description(&par2_file_id).unwrap().hash_full,
        "precondition: the unrelated PAR2 digest must not describe the payload"
    );
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        unrelated_par2_filename,
        &unrelated_par2,
    )
    .await;
    // The payload deliberately has no digest: its grid is the only evidence.
    // The completed PAR2 metadata carries a digest but is not described by this
    // recovery set, so it must not change the payload's attribution.
    set_measured_md5(&mut pipeline, job_id, 1, &unrelated_par2);

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    for (index, slice) in payload.chunks(32).enumerate() {
        let offset = (index as u64) * 32;
        let crc = par2_rs::checksum::crc32(slice);
        pipeline.note_block_crc_segments(
            file_id,
            offset,
            slice.len() as u64,
            crc,
            true,
            false,
            &[weaver_yenc::Segment {
                file_offset: offset,
                len: slice.len() as u64,
                crc32: crc,
            }],
        );
    }
    assert!(
        pipeline
            .block_crc_verdicts(file_id)
            .is_some_and(|verdicts| {
                verdicts.len() == 2
                    && verdicts.values().all(|verdict| {
                        matches!(
                            verdict,
                            crate::pipeline::integrity::BlockVerdict::Intact {
                                independently_covered: true
                            }
                        )
                    })
            }),
        "precondition: both slices must close Intact with independent coverage"
    );

    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    let payload_path = working_dir.join(payload_filename);
    let hidden_path = working_dir.join("payload-hidden-during-quick-verify");
    std::fs::rename(&payload_path, &hidden_path).unwrap();
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();
    let (_, _, evidence) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .unwrap()
        .unwrap();
    std::fs::rename(hidden_path, payload_path).unwrap();
    assert_eq!(
        evidence,
        QuickPar2Evidence::Grid,
        "grid-only quick verification must succeed while the payload path is unreadable"
    );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    // No digest was ever computed or persisted, and nothing may re-read the
    // payload: the grid alone quick-verifies the file.
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

#[tokio::test]
async fn grid_quick_completion_survives_encoded_nzb_declared_sizes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30181);
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    // Production shape: the NZB declares the yEnc-ENCODED article size, ~3%
    // larger than the decoded payload PAR2 describes. The grid arm must
    // compare decoded lengths, never the declared total.
    let declared = payload.len() as u32 + 37;
    let file_id =
        grid_verified_direct_job(&mut pipeline, job_id, "payload.mkv", &payload, declared).await;
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert_ne!(
            file.total_bytes(),
            payload.len() as u64,
            "fixture must actually be encoded-shaped"
        );
        assert_eq!(file.received_bytes(), payload.len() as u64);
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn grid_quick_match_with_agreeing_measured_md5_still_quick_verifies() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30182);
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    grid_verified_direct_job(
        &mut pipeline,
        job_id,
        "payload.mkv",
        &payload,
        payload.len() as u32,
    )
    .await;
    persist_completed_file_hash(&pipeline, job_id, 0, "payload.mkv", &payload).await;

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn grid_quick_match_contradicted_by_measured_md5_goes_authoritative() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30183);
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    grid_verified_direct_job(
        &mut pipeline,
        job_id,
        "payload.mkv",
        &payload,
        payload.len() as u32,
    )
    .await;
    // A trusted measured digest — the shape a duplicate-triggered re-read
    // leaves behind — that contradicts the description the grid selected.
    // CRC evidence must not override the stronger instrument.
    persist_completed_file_hash(
        &pipeline,
        job_id,
        0,
        "payload.mkv",
        b"other content entirely",
    )
    .await;

    pipeline.check_job_completion(job_id).await;

    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
}

#[tokio::test]
async fn grid_quick_path_refuses_when_a_slice_lacks_independent_coverage() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30179);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Grid Quick Verify Unverified Slice",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let mut par2_set = placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]);
    par2_set.slice_size = 32;
    let par2_file_id = par2_set.recovery_file_ids[0];
    let slice_checksums: Vec<par2_rs::SliceChecksum> = payload
        .chunks(32)
        .map(|slice| {
            let mut state = par2_rs::SliceChecksumState::new();
            state.update(slice);
            let (crc32, md5) = state.finalize(Some(32));
            par2_rs::SliceChecksum { crc32, md5 }
        })
        .collect();
    par2_set
        .slice_checksums
        .insert(par2_file_id, slice_checksums);
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    // The second article's pCRC never verified: its slice closes Intact but
    // without independent coverage, so the grid cannot vouch for the file.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    for (index, slice) in payload.chunks(32).enumerate() {
        let offset = (index as u64) * 32;
        let crc = par2_rs::checksum::crc32(slice);
        pipeline.note_block_crc_segments(
            file_id,
            offset,
            slice.len() as u64,
            crc,
            index == 0,
            false,
            &[weaver_yenc::Segment {
                file_offset: offset,
                len: slice.len() as u64,
                crc32: crc,
            }],
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

    // With no digest anywhere and the grid short of the bar, the file is not
    // quick-verified: the authoritative pass runs — and, reading genuinely
    // clean bytes, is entitled to verify the job the slow way.
    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
}

#[tokio::test]
async fn post_repair_refresh_replaces_stale_streamed_digests_with_verified_ones() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30180);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..48u32).map(|value| (value % 249) as u8).collect();
    let spec = standalone_job_spec(
        "Post Repair Hash Refresh",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_set = placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]);
    let par2_file_id = par2_set.recovery_file_ids[0];

    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    // The digest streamed BEFORE repair rewrote the file: it describes bytes
    // that are gone.
    let pre_rewrite_bytes = b"content the repair replaced";
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, pre_rewrite_bytes).await;

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: payload_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    pipeline
        .refresh_authoritative_verified_hashes(job_id, &par2_set, &verification)
        .await
        .unwrap();

    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(&payload)),
        "the authoritative post-repair digest must replace the stale streamed one"
    );
    assert_ne!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(pre_rewrite_bytes))
    );
}

#[tokio::test]
async fn post_repair_refresh_prefers_the_renamed_path_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30184);
    let disk_filename = "obfuscated.bin";
    let payload: Vec<u8> = (0..48u32).map(|value| (value % 249) as u8).collect();
    let spec = standalone_job_spec(
        "Post Repair Renamed Mapping",
        &[(disk_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    // The description knows the file by its correct name; verification found
    // the content living under the obfuscated on-disk name.
    let par2_set = placement_par2_file_set(&[("correct.mkv".to_string(), payload.clone())]);
    let par2_file_id = par2_set.recovery_file_ids[0];
    write_and_complete_file(&mut pipeline, job_id, 0, disk_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, disk_filename, b"stale pre-repair").await;

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: "correct.mkv".to_string(),
            status: par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from(disk_filename)),
            valid_slices: vec![true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    pipeline
        .refresh_authoritative_verified_hashes(job_id, &par2_set, &verification)
        .await
        .unwrap();

    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(&payload)),
        "the renamed-path identity must resolve and attach the verified digest"
    );
}

#[tokio::test]
async fn post_repair_refresh_skips_contested_and_ambiguous_identities() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30185);
    let payload: Vec<u8> = (0..48u32).map(|value| (value % 249) as u8).collect();
    // Two assembly files share one filename: any name-based resolution of
    // that alias is ambiguous by construction.
    let spec = standalone_job_spec(
        "Post Repair Ambiguous Aliases",
        &[
            ("same.bin".to_string(), payload.len() as u32),
            ("same.bin".to_string(), payload.len() as u32),
            ("unique.bin".to_string(), payload.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let par2_set = placement_par2_file_set(&[
        ("same.bin".to_string(), payload.clone()),
        ("unique.bin".to_string(), payload.clone()),
    ]);
    for (index, name) in [(0u32, "same.bin"), (1, "same.bin"), (2, "unique.bin")] {
        write_and_complete_file(&mut pipeline, job_id, index, name, &payload).await;
        persist_completed_file_hash(&pipeline, job_id, index, name, b"stale pre-repair").await;
    }

    let complete = |file_id, filename: &str| par2_rs::verify::FileVerification {
        file_id,
        filename: filename.to_string(),
        status: par2_rs::verify::FileStatus::Complete,
        valid_slices: vec![true],
        missing_slice_count: 0,
    };
    // Entry 1 resolves an ambiguous alias; entries 2 and 3 both claim the
    // one unambiguous file. Nothing may attach anywhere.
    let verification = par2_rs::VerificationResult {
        files: vec![
            complete(par2_set.recovery_file_ids[0], "same.bin"),
            complete(par2_set.recovery_file_ids[1], "unique.bin"),
            complete(par2_set.recovery_file_ids[0], "unique.bin"),
        ],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    pipeline
        .refresh_authoritative_verified_hashes(job_id, &par2_set, &verification)
        .await
        .unwrap();

    let stale = par2_rs::checksum::md5(b"stale pre-repair");
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    for index in 0..3u32 {
        assert_eq!(
            trusted.get(&index).copied(),
            Some(stale),
            "file {index}: ambiguity must leave existing digests untouched"
        );
    }
}

#[tokio::test]
async fn post_repair_refresh_requires_the_described_length_on_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30186);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..48u32).map(|value| (value % 249) as u8).collect();
    let mut described = payload.clone();
    described.extend_from_slice(b"tail the disk file does not have");
    let spec = standalone_job_spec(
        "Post Repair Length Gate",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let par2_set = placement_par2_file_set(&[(payload_filename.to_string(), described.clone())]);
    let par2_file_id = par2_set.recovery_file_ids[0];
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, b"stale pre-repair").await;

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: payload_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    pipeline
        .refresh_authoritative_verified_hashes(job_id, &par2_set, &verification)
        .await
        .unwrap();

    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(b"stale pre-repair")),
        "a length disagreement must not attach the described digest"
    );
}

#[tokio::test]
async fn stale_persisted_digest_must_not_outrank_the_current_runtime_generation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30187);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Runtime Generation Outranks Persisted",
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

    // Generation 1: the file completed clean once; its digest — which equals
    // the description — was persisted as trusted.
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;

    // A duplicate then physically rewrote AND extended the file. Completion
    // re-read the new generation into the runtime, but persisting the
    // replacement row failed (the worker logs and carries on), so the
    // database still holds the stale generation.
    let mut rewritten = payload.clone();
    rewritten[7] ^= 0xFF;
    rewritten.extend_from_slice(b"duplicate tail growth");
    tokio::fs::write(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .working_dir
            .join(payload_filename),
        &rewritten,
    )
    .await
    .unwrap();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline
        .ensure_par2_runtime(job_id)
        .completed_checksums
        .insert(
            file_id,
            crate::pipeline::CompletedFileChecksum {
                md5: Some(par2_rs::checksum::md5(&rewritten)),
                crc32: par2_rs::checksum::crc32(&rewritten),
                all_parts_crc_verified: false,
            },
        );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;

    // The current generation disagrees with the description; the stale
    // matching row must not quick-verify over it.
    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn an_unavailable_current_generation_must_not_revive_the_persisted_digest() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30188);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Unavailable Generation No Revival",
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

    // The current generation's finalize failed: the worker records exactly
    // this sentinel. The older persisted row describes bytes that may be
    // gone and must not be revived to quick-verify.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline
        .ensure_par2_runtime(job_id)
        .completed_checksums
        .insert(
            file_id,
            crate::pipeline::CompletedFileChecksum {
                md5: None,
                crc32: 0,
                all_parts_crc_verified: false,
            },
        );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;

    // The stale row must not produce a hash-only quick pass; the
    // authoritative pass runs instead — and, reading genuinely clean bytes,
    // is entitled to verify the job the slow way.
    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
}

#[tokio::test]
async fn post_repair_refresh_resolves_renamed_results_against_current_names_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30189);
    let payload: Vec<u8> = (0..48u32).map(|value| (value % 249) as u8).collect();
    let mut other_content = payload.clone();
    other_content[3] ^= 0x55; // same length, different bytes
    let spec = standalone_job_spec(
        "Renamed Resolution Current Names Only",
        &[
            ("x.bin".to_string(), payload.len() as u32),
            ("c-file.bin".to_string(), other_content.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Post-placement reality: the verified file now lives at its correct
    // name "b.mkv"; the path the pre-plan verification saw it at — "a.mkv" —
    // is no longer any of its aliases. A second, unrelated file happens to
    // have been POSTED as "a.mkv" (immutable source alias) and has the same
    // length as the description.
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "x.bin".to_string(),
                current_filename: "b.mkv".to_string(),
                canonical_filename: Some("b.mkv".to_string()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 1,
                source_filename: "a.mkv".to_string(),
                current_filename: "c-file.bin".to_string(),
                canonical_filename: None,
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();
    write_and_complete_file(&mut pipeline, job_id, 0, "b.mkv", &payload).await;
    write_and_complete_file(&mut pipeline, job_id, 1, "c-file.bin", &other_content).await;
    persist_completed_file_hash(&pipeline, job_id, 0, "b.mkv", b"stale pre-repair").await;
    persist_completed_file_hash(&pipeline, job_id, 1, "c-file.bin", b"stale pre-repair").await;

    let par2_set = placement_par2_file_set(&[("b.mkv".to_string(), payload.clone())]);
    let par2_file_id = par2_set.recovery_file_ids[0];
    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: "b.mkv".to_string(),
            status: par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from("a.mkv")),
            valid_slices: vec![true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    pipeline
        .refresh_authoritative_verified_hashes(job_id, &par2_set, &verification)
        .await
        .unwrap();

    let stale = par2_rs::checksum::md5(b"stale pre-repair");
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&1).copied(),
        Some(stale),
        "a stale pre-plan path must never attach the digest to an unrelated file via its source alias"
    );
    assert_eq!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(&payload)),
        "the digest belongs to the file whose CURRENT name carries the verified content"
    );
}

#[tokio::test]
async fn unparseable_par2_index_is_skipped_and_the_job_keeps_serving() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30500);
    let payload_filename = "payload.mkv";
    let index_filename = "broken.par2";
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    // Structurally invalid PAR2 bytes: the same warn-and-continue arm that a
    // packet-scan `ResourceLimitExceeded` from a hostile index now lands in
    // (par2-rs 0.5 proves limit errors at the crate level; this pins what
    // weaver does with ANY parse-side error from that seam).
    let garbage = vec![0x5Au8; 4096];
    let spec = standalone_job_spec(
        "Unparseable PAR2 Keeps Serving",
        &[
            (payload_filename.to_string(), payload.len() as u32),
            (index_filename.to_string(), garbage.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file_like_decode_worker(&mut pipeline, job_id, 1, index_filename, &garbage)
        .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    pipeline.try_load_par2_metadata(job_id, file_id).await;

    // The broken index installs nothing and poisons nothing: no recovery set,
    // and the data file still downloads and completes normally.
    assert!(pipeline.par2_set(job_id).is_none());
    write_and_complete_file_like_decode_worker(
        &mut pipeline,
        job_id,
        0,
        payload_filename,
        &payload,
    )
    .await;
    // Retirement from the active map is fine — that is a completed job. What
    // the broken index must never cause is a failure.
    let drained = drain_job_events(&mut events, job_id);
    assert!(
        !drained
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "{drained:?}"
    );
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
    assert!(matches!(
        restored
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&0))
            .map(|file| &file.discovery),
        Some(Par2DiscoveryState::Parsed { .. })
    ));
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
    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_rs::FileId::from_bytes([0u8; 16]),
            filename: filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: Vec::new(),
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(
            job_id,
            &verification,
            &HashSet::new(),
        ),
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
            .verified_complete_archive_file_ids_needing_refresh(
                job_id,
                &verification,
                &HashSet::new(),
            )
            .is_empty(),
        "existing topology should avoid a redundant refresh for unchanged complete files"
    );

    // The same unchanged file, named by the repair as one it rewrote, is
    // refreshed anyway: the plan that existed is the one the repair invalidated.
    let rewritten: HashSet<par2_rs::FileId> = verification
        .files
        .iter()
        .map(|file| file.file_id)
        .collect::<HashSet<_>>();
    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(
            job_id,
            &verification,
            &rewritten,
        ),
        vec![file_id],
        "a file the repair rewrote must refresh even though its set already has a topology"
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
    // Two: the pre-repair authoritative pass, then the post-repair pass that
    // re-reads what the repair installed. Every repair path runs both passes.
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        2
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

/// The post-repair pass reads the file the repair rewrote and nothing else.
#[tokio::test]
async fn post_repair_verification_reads_only_the_files_the_repair_rewrote() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30288);
    let job_name = "Selective Post Repair Verification";
    let (working_dir, damaged_original, intact_original) =
        two_payload_repair_job(&mut pipeline, job_id, job_name).await;
    // The re-entry that routes a job through verify-then-repair: PAR2 already
    // ruled the job clean once, extraction then failed on the archive, and the
    // job comes back through the gate with its verdict closed.
    pipeline.par2_verified.insert(job_id);
    pipeline
        .failed_extractions
        .insert(job_id, ["sample.mkv".to_string()].into_iter().collect());

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.par2_post_repair_read_splits,
        vec![(1usize, 1usize)],
        "one file carried from the pre-repair pass, one file read back — the \
         intact payload must not be re-hashed just because its neighbour was \
         repaired. splits = {:?}",
        pipeline.par2_post_repair_read_splits
    );
    assert_eq!(drain_job_repair_complete(&mut repair_events, job_id), 1);
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "and the merged verdict has to clear the post-repair gate — a carried \
         entry that lost its Complete status would fail the job here; status = {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
    assert!(
        !pipeline.failed_extractions.contains_key(&job_id),
        "the post-repair arm's downstream work ran over the merged result"
    );
    assert_eq!(
        tokio::fs::read(working_dir.join("damaged.zip"))
            .await
            .unwrap(),
        damaged_original,
        "the repair really did rewrite the damaged payload"
    );
    assert_eq!(
        tokio::fs::read(working_dir.join("intact.mkv"))
            .await
            .unwrap(),
        intact_original
    );
}

/// The trade this seam accepts, pinned honestly rather than left implicit.
///
/// A file the repair did not touch is vouched by the pre-repair pass, which
/// read its bytes. If something outside this job rewrites that file in the
/// minutes between the two passes, the post-repair pass will not notice — it
/// is not asked to. This is the documented residual, not a bug: it is the same
/// window, and the same trust class, as an in-stream claim relied on across the
/// same interval. The test exists so the day someone changes it, they change it
/// deliberately.
#[tokio::test]
async fn post_repair_verification_accepts_a_file_corrupted_after_the_pre_repair_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30289);
    let (working_dir, _damaged_original, intact_original) = two_payload_repair_job(
        &mut pipeline,
        job_id,
        "Post Repair Verification Accepted Window",
    )
    .await;
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();
    let intact_file_id = par2_set.recovery_file_ids[1];
    let damaged_file_id = par2_set.recovery_file_ids[0];

    // Stand in for the pre-repair pass: the damaged payload was the write set,
    // the intact one was read and proved complete.
    let pre_repair = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: damaged_file_id,
                filename: "damaged.zip".to_string(),
                status: par2_rs::verify::FileStatus::Damaged(1),
                valid_slices: vec![true, false],
                missing_slice_count: 1,
            },
            par2_rs::verify::FileVerification {
                file_id: intact_file_id,
                filename: "intact.mkv".to_string(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
        ],
        recovery_blocks_available: par2_set.recovery_block_count(),
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: par2_set.recovery_block_count(),
        },
    };

    // The repair installs the file it rewrote...
    tokio::fs::write(working_dir.join("damaged.zip"), &_damaged_original)
        .await
        .unwrap();
    // ...and, in the same window, something outside this job destroys the file
    // the repair never touched.
    let mut corrupted = intact_original.clone();
    corrupted[..64].fill(0);
    tokio::fs::write(working_dir.join("intact.mkv"), &corrupted)
        .await
        .unwrap();

    let (merged, plan) = pipeline
        .verify_repaired_par2_files_with_placement(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
        )
        .await
        .unwrap();

    assert_eq!(
        pipeline.par2_post_repair_read_splits,
        vec![(1usize, 1usize)]
    );
    let intact_entry = merged
        .files
        .iter()
        .find(|file| file.file_id == intact_file_id)
        .unwrap();
    assert!(
        matches!(intact_entry.status, par2_rs::verify::FileStatus::Complete),
        "the corruption is NOT caught, by design: the entry carried is the one \
         the pre-repair pass made, and that pass read bytes which were sound at \
         the time"
    );
    let damaged_entry = merged
        .files
        .iter()
        .find(|file| file.file_id == damaged_file_id)
        .unwrap();
    assert!(
        matches!(damaged_entry.status, par2_rs::verify::FileStatus::Complete),
        "while the rewritten file WAS read back, and reports what is on disk now"
    );
    assert!(plan.swaps.is_empty() && plan.renames.is_empty());
}

/// A file the pre-repair verdict called `Renamed` is read back at its canonical
/// name, not carried.
///
/// The repairer treats a misplaced file as work: it is not complete at the path
/// its description names, so the repair copies the bytes onto that path and
/// moves whatever held the name aside. Carrying the pre-repair entry through
/// that would report a file as still misplaced after the repair had already
/// placed it, and hand the placement step a rename onto a name the repair had
/// just filled.
#[tokio::test]
async fn post_repair_verification_reads_back_a_renamed_file_at_its_canonical_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30290);
    let (working_dir, damaged_original, intact_original) = two_payload_repair_job(
        &mut pipeline,
        job_id,
        "Post Repair Verification Renamed Read Back",
    )
    .await;
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();
    let misplaced_file_id = par2_set.recovery_file_ids[0];
    let intact_file_id = par2_set.recovery_file_ids[1];

    // The pre-repair verdict for a set whose one fault was placement: the
    // repairer's scanner found the payload's content under another name.
    let pre_repair = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: misplaced_file_id,
                filename: "damaged.zip".to_string(),
                status: par2_rs::verify::FileStatus::Renamed(working_dir.join("elsewhere.bin")),
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: intact_file_id,
                filename: "intact.mkv".to_string(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
        ],
        recovery_blocks_available: par2_set.recovery_block_count(),
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    // What the repair leaves on disk: the content installed at the canonical
    // name, with the copy it was found under still sitting where it was.
    tokio::fs::write(working_dir.join("damaged.zip"), &damaged_original)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("elsewhere.bin"), &damaged_original)
        .await
        .unwrap();

    let (merged, plan) = pipeline
        .verify_repaired_par2_files_with_placement(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
        )
        .await
        .unwrap();

    assert_eq!(
        pipeline.par2_post_repair_read_splits,
        vec![(1usize, 1usize)],
        "the misplaced file belongs to the rewritten set, not the carried one — \
         only the file that was already complete at its canonical name may be \
         carried. splits = {:?}",
        pipeline.par2_post_repair_read_splits
    );
    let placed = merged
        .files
        .iter()
        .find(|file| file.file_id == misplaced_file_id)
        .unwrap();
    assert!(
        matches!(placed.status, par2_rs::verify::FileStatus::Complete),
        "read at the canonical name the repair installed it to, it is complete; \
         status = {:?}",
        placed.status
    );
    assert!(
        plan.swaps.is_empty() && plan.renames.is_empty(),
        "so the derived plan has nothing left to move; plan = {plan:?}"
    );
    assert!(
        !merged.needs_repair(),
        "and the merged verdict clears the post-repair gate"
    );
    assert_eq!(
        tokio::fs::read(working_dir.join("intact.mkv"))
            .await
            .unwrap(),
        intact_original
    );
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
    // Two: the pre-repair authoritative pass, then the post-repair pass that
    // re-reads what the repair installed. Every repair path runs both passes.
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        2
    );
    assert_eq!(drain_job_repair_complete(&mut repair_events, job_id), 1);
    assert_eq!(pipeline.par2_lower_bound_preflight_calls, 0);
    assert_eq!(pipeline.par2_authoritative_verify_calls, 0);
    // The post-repair re-read of what the repair installed. Selective: it
    // reads only the files the repair rewrote, not the whole set.
    assert_eq!(pipeline.par2_selective_verify_calls, 1);
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
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 2,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
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
    // Both authoritative passes are detached reads; each one's verdict re-enters
    // the completion check as a message.
    settle_par2_analysis_work(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    // Two: the pre-repair authoritative pass, then the post-repair pass that
    // re-reads what the repair installed. Every repair path runs both passes.
    assert_eq!(
        drain_job_verification_started(&mut verify_events, job_id),
        2
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
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
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
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: recovery_file_id,
                segment_number: 1,
            },
            message_id: MessageId::new("cancel-promoted-parked@example.com"),
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
        .handle_command(SchedulerCommand::CancelJob {
            job_id,
            origin: crate::jobs::handle::CancellationOrigin::User,
            reply,
        })
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
    // The promotion is decided by the detached read's verdict.
    settle_par2_analysis_work(&mut pipeline).await;
    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.download_queue.has_recovery_work())
    );

    let queued_recovery = {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.drain_all()
    };
    assert_eq!(queued_recovery.len(), 1);
    pipeline
        .pending_released_download_results_by_job
        .insert(job_id, 1);
    pipeline.check_job_completion(job_id).await;
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );

    pipeline
        .pending_released_download_results_by_job
        .remove(&job_id);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        for work in queued_recovery {
            state.download_queue.push(work);
        }
    }
    pipeline.check_job_completion(job_id).await;
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

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
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 1000,
        byte_estimate: 64,
        retry_count: 1,
        is_recovery: true,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
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
    assert!(queued.completion_critical);
}
