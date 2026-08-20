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
    let payload: Vec<u8> = (0..64u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Metadata Early Clean Grid Quick Verify",
        &[(payload_filename.to_string(), payload.len() as u32)],
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
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    // Deliberately NO persisted digest: the grid is the only evidence.

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

async fn grid_verified_direct_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload_filename: &str,
    payload: &[u8],
    declared_size: u32,
) -> NzbFileId {
    let spec = standalone_job_spec(
        "Grid Verified Direct Job",
        &[(payload_filename.to_string(), declared_size)],
    );
    insert_active_job(pipeline, job_id, spec).await;

    let mut par2_set = placement_par2_file_set(&[(payload_filename.to_string(), payload.to_vec())]);
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
    install_test_par2_runtime(pipeline, job_id, par2_set, &[]);

    write_and_complete_file(pipeline, job_id, 0, payload_filename, payload).await;

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
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    file_id
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
    // Two: the pre-repair authoritative pass, then the post-repair pass that
    // re-reads what the repair installed. Every repair path runs the second
    // one now — it is the phase SABnzbd shows as "verifying repaired files"
    // and NZBGet as `ptVerifyingRepaired`.
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

/// A two-payload job whose first file is damaged and whose second is intact,
/// wired the same way as the single-payload repair test above.
///
/// The damaged payload carries a `Zip` archive topology so the job's clean-PAR2
/// integrity gate reads `StrongDecode`. That is what routes it through the
/// verify-then-repair branch — the one whose post-repair pass this exercises —
/// rather than the repairer-analysis branch a bare payload takes.
///
/// Returns the working directory plus the two payloads' original bytes.
async fn two_payload_repair_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> (PathBuf, Vec<u8>, Vec<u8>) {
    let damaged_filename = "damaged.zip";
    let intact_filename = "intact.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let damaged_original: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let intact_original: Vec<u8> = (0..128u32)
        .map(|value| (value % 241) as u8 ^ 0x5A)
        .collect();
    let mut damaged_on_disk = damaged_original.clone();
    for byte in &mut damaged_on_disk[64..128] {
        *byte = 0;
    }

    let par2_bytes = build_test_par2_index_for_files(
        &[
            (damaged_filename, &damaged_original),
            (intact_filename, &intact_original),
        ],
        64,
    );
    let recovery_bytes = vec![0xAA; 64];
    let payload_segments = |prefix: &str| {
        vec![
            segment_spec! {
                number: 0,
                bytes: 64,
                message_id: format!("{prefix}-0@example.com"),
            },
            segment_spec! {
                number: 1,
                bytes: 64,
                message_id: format!("{prefix}-1@example.com"),
            },
        ]
    };
    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes: (damaged_original.len()
            + intact_original.len()
            + par2_bytes.len()
            + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: damaged_filename.to_string(),
                role: FileRole::from_filename(damaged_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: payload_segments("selective-damaged"),
            },
            FileSpec {
                filename: intact_filename.to_string(),
                role: FileRole::from_filename(intact_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: payload_segments("selective-intact"),
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "selective-index@example.com".to_string(),
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
                    message_id: "selective-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(damaged_filename), &damaged_on_disk)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(intact_filename), &intact_original)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..2u32 {
            let file_id = NzbFileId { job_id, file_index };
            let file = state.assembly.file_mut(file_id).unwrap();
            file.commit_segment(0, 64).unwrap();
            file.commit_segment(1, 64).unwrap();
        }
        state.assembly.set_archive_topology(
            damaged_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([(damaged_filename.to_string(), 0)]),
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
    write_and_complete_file(pipeline, job_id, 2, index_filename, &par2_bytes).await;
    write_and_complete_file(pipeline, job_id, 3, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &[
                (damaged_filename, &damaged_original),
                (intact_filename, &intact_original),
            ],
            64,
            1,
        ),
        &[
            (2, index_filename, 0, false),
            (3, recovery_filename, 1, true),
        ],
    );

    (working_dir, damaged_original, intact_original)
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
    // re-reads what the repair installed. Every repair path runs the second
    // one now — it is the phase SABnzbd shows as "verifying repaired files"
    // and NZBGet as `ptVerifyingRepaired`.
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
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
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    // Two: the pre-repair authoritative pass, then the post-repair pass that
    // re-reads what the repair installed. Every repair path runs the second
    // one now — it is the phase SABnzbd shows as "verifying repaired files"
    // and NZBGet as `ptVerifyingRepaired`.
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
            avoid_server: None,
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
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
        groups: vec!["alt.binaries.test".to_string()],
        priority: 1000,
        byte_estimate: 64,
        retry_count: 1,
        is_recovery: true,
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
            avoid_server: None,
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
            avoid_server: None,
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

/// A real NZB's `<segment bytes=…>` is the *yEnc-encoded* article size, about
/// 3% larger than the decoded payload PAR2 describes. Every live-PAR2 fixture
/// declares inflated sizes so the declared total never equals the decoded
/// length — the shape production always has.
fn yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

fn split_payload_job_split(payload_len: u32) -> u32 {
    payload_len * 3 / 8
}

fn split_payload_par2_job_spec(
    name: &str,
    payload_filename: &str,
    payload_len: u32,
    index_filename: &str,
    index_len: u32,
) -> JobSpec {
    let first_segment = split_payload_job_split(payload_len);
    let declared_first = yenc_declared_bytes(first_segment);
    let declared_second = yenc_declared_bytes(payload_len - first_segment);
    let declared_index = yenc_declared_bytes(index_len);
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (declared_first + declared_second + declared_index) as u64,
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
                        bytes: declared_first,
                        message_id: "live-par2-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: declared_second,
                        message_id: "live-par2-payload-1@example.com".to_string(),
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
                    bytes: declared_index,
                    message_id: "live-par2-index@example.com".to_string(),
                }],
            },
        ],
    }
}

async fn submit_split_payload(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload_filename: &str,
    written_payload: &[u8],
) {
    let payload_file = NzbFileId {
        job_id,
        file_index: 0,
    };
    // Segment 0 stops mid-block, so block 0 is staged across the boundary and
    // block 1 is claimed whole.
    let split = split_payload_job_split(written_payload.len() as u32) as usize;
    submit_decoded_segment(
        pipeline,
        payload_file,
        0,
        0,
        &written_payload[..split],
        payload_filename,
        None,
    )
    .await;
    submit_decoded_segment(
        pipeline,
        payload_file,
        1,
        split as u64,
        &written_payload[split..],
        payload_filename,
        None,
    )
    .await;
}

async fn submit_par2_index(
    pipeline: &mut Pipeline,
    job_id: JobId,
    index_filename: &str,
    par2_bytes: &[u8],
) {
    submit_decoded_segment(
        pipeline,
        NzbFileId {
            job_id,
            file_index: 1,
        },
        0,
        0,
        par2_bytes,
        index_filename,
        None,
    )
    .await;
}

async fn drain_job_to_completion(pipeline: &mut Pipeline, job_id: JobId) {
    {
        // A damaged job can already have failed and been retired by the time
        // the last segment commits; nothing left to drain.
        let Some(state) = pipeline.jobs.get_mut(&job_id) else {
            return;
        };
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;
}

/// A clean two-segment payload plus its index verifies and completes.
///
/// The payload's first segment stops mid-block, so block 0 is only ever staged
/// across an article boundary while block 1 closes whole — the shape that keeps
/// this honest about a job the in-stream grid cannot claim outright.
#[tokio::test]
async fn a_clean_job_verifies_and_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30461);
    let payload_filename = "Silver.Horizon.S01E01.mkv";
    let index_filename = "Silver.Horizon.S01E01.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let par2_bytes = build_test_par2_index(payload_filename, &payload, 64);
    let spec = split_payload_par2_job_spec(
        "Clean Split Payload",
        payload_filename,
        payload.len() as u32,
        index_filename,
        par2_bytes.len() as u32,
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    submit_par2_index(&mut pipeline, job_id, index_filename, &par2_bytes).await;
    submit_split_payload(&mut pipeline, job_id, payload_filename, &payload).await;
    drain_job_to_completion(&mut pipeline, job_id).await;

    assert!(pipeline.par2_verified.contains(&job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

/// The same job with one payload byte flipped reaches the authoritative
/// analyzer exactly once and fails as unrepairable — no quick arm may conclude
/// verification over bytes that contradict the recovery set.
#[tokio::test]
async fn a_damaged_job_runs_the_authoritative_analyzer_and_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30463);
    let payload_filename = "Silver.Horizon.S01E02.mkv";
    let index_filename = "Silver.Horizon.S01E02.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged = payload.clone();
    damaged[70] ^= 0xFF;
    let par2_bytes = build_test_par2_index(payload_filename, &payload, 64);
    let spec = split_payload_par2_job_spec(
        "Damaged Split Payload",
        payload_filename,
        payload.len() as u32,
        index_filename,
        par2_bytes.len() as u32,
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    submit_par2_index(&mut pipeline, job_id, index_filename, &par2_bytes).await;
    submit_split_payload(&mut pipeline, job_id, payload_filename, &damaged).await;
    drain_job_to_completion(&mut pipeline, job_id).await;

    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { error }) if error.contains("not repairable")
    ));
}

#[tokio::test]
async fn waiting_on_present_volumes_is_not_repair_ready_until_a_volume_is_truly_absent() {
    // `WaitingForVolumes` covers two different situations, and PAR2
    // repair-readiness must only fire for one of them. A set mid
    // swap-correction waits on volume *numbers* while every actual volume sits
    // parsed on disk under mismatched numbering — that wait is answered by the
    // cached-header retry, and treating it as missing-volume repair readiness
    // sent a swap job into damaged-path analysis, promoted recovery blocks it
    // had no use for, and emitted verification events its fixture forbids. A
    // volume that is genuinely absent — no facts, no file — is the shape where
    // PAR2 really is the only move.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30031);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "RAR Swap Waiting",
            &[
                ("show.part1.rar".to_string(), 512),
                ("show.part2.rar".to_string(), 512),
                ("show.part3.rar".to_string(), 512),
            ],
        ),
    )
    .await;
    for filename in ["show.part1.rar", "show.part2.rar", "show.part3.rar"] {
        tokio::fs::write(working_dir.join(filename), b"volume-bytes")
            .await
            .unwrap();
    }

    let set_key = (job_id, "show".to_string());
    pipeline.rar_sets.insert(
        set_key.clone(),
        crate::pipeline::archive::rar_state::RarSetState {
            facts: [
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
                (2u32, dummy_rar_volume_facts(2)),
            ]
            .into_iter()
            .collect(),
            volume_files: [
                (0u32, "show.part1.rar".to_string()),
                (1u32, "show.part2.rar".to_string()),
                (2u32, "show.part3.rar".to_string()),
            ]
            .into_iter()
            .collect(),
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: Default::default(),
                // The swap transient: waiting on 1..=3 while parsed volumes are
                // 0..=2 — the waited numbers that exist are all present, and 3
                // is a phantom of the mislabeling.
                waiting_on_volumes: [1, 2, 3].into_iter().collect(),
                deletion_eligible: Default::default(),
                delete_decisions: Default::default(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: Default::default(),
                    complete_volumes: Default::default(),
                    expected_volume_count: None,
                    members: Vec::new(),
                    unresolved_spans: Vec::new(),
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(
        pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id),
        "the broad phase-based predicate must still read this as waiting"
    );
    // `insert_active_job` queued the spec's segments, so the pipeline still
    // owes this job payload work — and mid-download, an absent volume is just
    // a volume that has not arrived yet. Nothing in `WaitingForVolumes` may
    // qualify while anything is en route (the demotion-refetch and swap
    // transients both fired the predicate 10 seconds into a job this way).
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "pending download work means absence proves nothing yet"
    );

    // Quiet the pipeline; the rest of the contract is about exhaustion.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
    }
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "present waiting volumes mean the volume-0 retry is owed its chance — \
         not PAR2"
    );

    // Same set once the wait is only on a volume nothing can produce — the
    // missing-middle shape. Now readiness must fire.
    if let Some(plan) = pipeline
        .rar_sets
        .get_mut(&set_key)
        .and_then(|set_state| set_state.plan.as_mut())
    {
        plan.waiting_on_volumes = [3].into_iter().collect();
    }
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "a wait no retry can answer is exactly the repair-readiness shape"
    );

    // And the same missing-middle shape stops qualifying the moment any
    // pipeline work reappears — `health_probing` here stands in for any of
    // the pending-work arms.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.health_probing = true;
    }
    assert!(
        !pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "absence while anything is en route is not absence"
    );

    // AwaitingRepair qualifies with an empty waiting list — the livelocked
    // small-repair family sits exactly there — and it stays unconditional:
    // the extraction machinery itself concluded only repair moves the set,
    // pending work or not.
    if let Some(plan) = pipeline
        .rar_sets
        .get_mut(&set_key)
        .and_then(|set_state| set_state.plan.as_mut())
    {
        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair;
        plan.waiting_on_volumes = Default::default();
    }
    assert!(
        pipeline.job_has_live_rar_waiting_for_absent_volumes(job_id),
        "AwaitingRepair is unconditional — even while the pipeline is busy"
    );
}

/// The job 11737 shape, end to end.
///
/// A standalone payload with one article that never arrived, a recovery block
/// that covers the hole, and a PAR2 repair that puts the file right. The
/// article bitmap is *not* backfilled by the repair — nothing rewrites history
/// — so the job used to fail its final completeness veto despite holding a
/// verified, byte-correct output.
///
/// Once PAR2 has repaired and re-verified a protected output, that verification
/// is authoritative and the bitmap is diagnostic history. This is exactly what
/// NZBGet pins in `test_parchecker_repair`, which makes a segment unavailable
/// and asserts `SUCCESS/PAR`.
#[tokio::test]
async fn missing_article_repaired_by_par2_completes_despite_incomplete_bitmap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(11737);
    let payload_filename = "silver.horizon.mkv";
    let index_filename = "silver.horizon.par2";
    let recovery_filename = "silver.horizon.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    // The second article never landed, so its slice is a hole on disk.
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Silver Horizon Missing Article".to_string(),
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
                        message_id: "silver-horizon-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "silver-horizon-1@example.com".to_string(),
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
                    message_id: "silver-horizon-index@example.com".to_string(),
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
                    message_id: "silver-horizon-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        // Only the first article ever arrived. This is the whole point of the
        // test: the bitmap stays one segment short for the rest of the job.
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(payload_file_id)
            .unwrap()
            .is_complete(),
        "precondition: the payload must start with a hole in its article bitmap"
    );
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
        Some(JobStatus::Complete),
        "a repaired, re-verified payload must not fail for its article bitmap; {}",
        debug_job_state(&pipeline, job_id)
    );

    let drained = drain_job_events(&mut events, job_id);
    let repair_completes: Vec<u32> = drained
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RepairComplete {
                slices_repaired, ..
            } => Some(*slices_repaired),
            _ => None,
        })
        .collect();
    assert_eq!(
        repair_completes.len(),
        1,
        "exactly one RepairComplete; events = {drained:?}"
    );
    // Counted from the pre-repair verdict. Read off the post-repair result it
    // would always be zero, because a repair that succeeded leaves no missing
    // blocks behind to count.
    assert_eq!(
        repair_completes[0], 1,
        "the one reconstructed slice must be reported"
    );

    let repair_complete_at = drained
        .iter()
        .position(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
        .unwrap();
    assert!(
        !drained[repair_complete_at..]
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "RepairComplete must never be followed by the job failing; events = {drained:?}"
    );

    // The promotion itself is durable, so a restart cannot resurrect the hole:
    // the completed row carries the recovery set's digest, not the pre-repair
    // bytes. (The in-memory assembly is gone by now — a completed job is
    // dropped from the map, which is itself proof the veto let it through.)
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(
        trusted.get(&payload_file_id.file_index).copied(),
        Some(par2_rs::checksum::md5(&original_payload)),
        "the repaired payload must persist its verified digest"
    );

    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Silver Horizon Missing Article",
        ));
    assert_eq!(
        tokio::fs::read(output_dir.join(payload_filename))
            .await
            .unwrap(),
        original_payload,
        "the delivered file must be the repaired bytes"
    );
}

/// A payload posted under an obfuscated name reconciles through PAR2 content
/// identity, not string equality.
///
/// The reconciler this replaces matched the verification's filename against the
/// assembly's stored names and did nothing at all when no exact string matched:
/// no promotion, no error, just a silently unreconciled file for the
/// completeness veto downstream to fail the whole job on. An obfuscated post is
/// exactly the case that never matches — the subject lies about the name and
/// tells the truth about the bytes — so the binding has to ask the bytes.
#[tokio::test]
async fn obfuscated_payload_reconciles_through_par2_content_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30311);
    let posted_filename = "a7f3e91c9b2d4e6f.bin";
    let described_filename = "Silver Horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: "Obfuscated PAR2 Reconciliation".to_string(),
        password: None,
        total_bytes: payload.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: posted_filename.to_string(),
            role: FileRole::from_filename(posted_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "obfuscated-0@example.com".to_string(),
                },
                segment_spec! {
                    number: 1,
                    bytes: 64,
                    message_id: "obfuscated-1@example.com".to_string(),
                },
            ],
        }],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join(posted_filename), &payload)
        .await
        .unwrap();

    let payload_file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(payload_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    // The bytes are what the obfuscation did not touch, and what the content
    // binding reads.
    pipeline
        .file_prefix_16k
        .insert(payload_file_id, payload.clone());

    let par2_set = build_repairable_par2_set(described_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: described_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .expect("reconciliation must not error");

    assert_eq!(
        report.completed, 1,
        "the verified payload must be promoted even though its posted name \
         matches no description; report = {report:?}"
    );
    assert!(
        report.unbound.is_empty(),
        "a content-bound file is not unbound; report = {report:?}"
    );
    assert!(report.contested.is_empty(), "report = {report:?}");
    assert!(report.length_mismatch.is_empty(), "report = {report:?}");
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(payload_file_id)
            .unwrap()
            .is_complete(),
        "promotion must fill the article bitmap"
    );
}

/// Two assembly files that both answer to one description bind to neither.
///
/// Content cannot break a tie that two files both satisfy, so the binding is
/// refused outright and named. The reconciler this replaces resolved the same
/// contest first-writer-wins — whichever file the iteration happened to reach
/// first was promoted, and the other silently was not.
#[tokio::test]
async fn contested_par2_binding_is_refused_and_named() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30312);
    let described_filename = "Silver Horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    // Two obfuscated posts of byte-identical content: both answer to the one
    // description, by content, equally well.
    let first_posted = "aaaa1111.bin";
    let second_posted = "bbbb2222.bin";

    let file_spec = |filename: &str, tag: &str| FileSpec {
        filename: filename.to_string(),
        role: FileRole::from_filename(filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 128,
            message_id: format!("contested-{tag}@example.com"),
        }],
    };
    let spec = JobSpec {
        name: "Contested PAR2 Binding".to_string(),
        password: None,
        total_bytes: (payload.len() * 2) as u64,
        category: None,
        metadata: vec![],
        files: vec![file_spec(first_posted, "a"), file_spec(second_posted, "b")],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for name in [first_posted, second_posted] {
        tokio::fs::write(working_dir.join(name), &payload)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    for file_index in 0..2u32 {
        pipeline
            .file_prefix_16k
            .insert(NzbFileId { job_id, file_index }, payload.clone());
    }

    let par2_set = build_repairable_par2_set(described_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: described_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .expect("a contest is reported, not an error from the pass itself");

    assert_eq!(
        report.completed, 0,
        "neither contender may be promoted on a guess; report = {report:?}"
    );
    assert_eq!(
        report.contested,
        vec![described_filename.to_string()],
        "the contest must name the description it could not resolve"
    );
}

/// A PAR2 verdict does not launder an unprotected file's missing articles.
///
/// The invariant is narrow on purpose: repair-and-reverify is authoritative for
/// the files the recovery set *describes*. A file the set never covered has no
/// verdict standing behind it, so an incomplete one is still a genuine download
/// failure — and the classification says which of the two it is, rather than
/// reporting both as one bare count.
#[tokio::test]
async fn unprotected_incomplete_file_still_fails_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30313);
    let protected_filename = "silver.horizon.mkv";
    let unprotected_filename = "extras.nfo";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: "Unprotected Missing File".to_string(),
        password: None,
        total_bytes: (payload.len() + 128) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: protected_filename.to_string(),
                role: FileRole::from_filename(protected_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "unprotected-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: unprotected_filename.to_string(),
                role: FileRole::from_filename(unprotected_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "unprotected-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "unprotected-nfo-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::write(working_dir.join(protected_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        // The NFO lost an article and nothing protects it.
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    // The recovery set covers the payload only.
    let par2_set = build_repairable_par2_set(protected_filename, &payload, 64, 1);
    let par2_file_id = par2_set.recovery_file_ids[0];
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let verification = par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_file_id,
            filename: protected_filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let error = pipeline
        .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2 test")
        .await
        .expect_err("an unprotected file with a hole must still fail the job");

    assert!(
        error.contains("unprotected"),
        "the failure must say the file was unprotected, not blame reconciliation: {error}"
    );
    assert!(
        error.contains(unprotected_filename),
        "the failure must name the file that could not be assembled: {error}"
    );
    assert!(
        !error.contains(protected_filename),
        "the repaired, verified payload must not be implicated: {error}"
    );
}
