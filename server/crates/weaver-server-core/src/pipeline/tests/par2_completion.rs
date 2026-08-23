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

/// An unprotected file short of articles is reported, and delivered anyway.
///
/// Job 10000 forced this: a 1.09 GB payload that PAR2 repaired and re-verified,
/// failed because a 738 KB `.nfo` no recovery set ever covered was missing a few
/// articles. Health 999. Both oracles ship that job — NZBGet's `FAILURE/HEALTH`
/// requires par to have been *skipped*, and SABnzbd never derives a failure from
/// missing articles at all — and weaver's final move relocates the working
/// directory wholesale, so the bytes reach the user regardless. Refusing the job
/// destroys a good download to report damage on a text file.
///
/// The distinction still has to survive in the *message*, because a protected
/// file left incomplete means something entirely different.
#[tokio::test]
async fn unprotected_incomplete_file_is_reported_but_does_not_fail_the_job() {
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

    pipeline
        .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2 test")
        .await
        .expect("an unprotected file short of articles must not fail the job");

    // The payload was promoted; only the uncovered NFO is still short.
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0
            })
            .unwrap()
            .is_complete(),
        "the PAR2-verified payload must still be promoted"
    );

    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "clean PAR2 test",
        )
        .expect("the uncovered file must still be reported");
    assert_eq!(
        report.unproven_protected, 0,
        "a file no recovery set covers is not a protected defect: {}",
        report.message
    );
    assert!(
        report.message.contains("unprotected"),
        "the report must say the file was unprotected, not blame reconciliation: {}",
        report.message
    );
    assert!(
        report.message.contains(unprotected_filename),
        "the report must name the file that came up short: {}",
        report.message
    );
    assert!(
        !report.message.contains(protected_filename),
        "the repaired, verified payload must not be implicated: {}",
        report.message
    );
}

/// Build a single-payload job whose article bitmap is one segment short, with a
/// PAR2 set describing the payload. Returns the working dir and the payload's
/// file id.
async fn incomplete_protected_payload_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    name: &str,
    payload_filename: &str,
    payload: &[u8],
) -> (PathBuf, NzbFileId) {
    let spec = JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: payload.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: payload_filename.to_string(),
            role: FileRole::from_filename(payload_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: format!("{name}-0@example.com"),
                },
                segment_spec! {
                    number: 1,
                    bytes: 64,
                    message_id: format!("{name}-1@example.com"),
                },
            ],
        }],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    (working_dir, file_id)
}

fn complete_verification_for(
    par2_set: &Par2FileSet,
    filename: &str,
) -> par2_rs::VerificationResult {
    par2_rs::VerificationResult {
        files: vec![par2_rs::verify::FileVerification {
            file_id: par2_set.recovery_file_ids[0],
            filename: filename.to_string(),
            status: par2_rs::verify::FileStatus::Complete,
            valid_slices: vec![true, true],
            missing_slice_count: 0,
        }],
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

/// A verdict vouching for bytes that are at neither name is refused.
///
/// The presence gate was relaxed to unblock direct-store, whose routing volumes
/// are verified through the set's own access layer and legitimately have no
/// file. Relaxing it for *every* binding went too far: an ordinary file that is
/// simply gone would be promoted to complete on the strength of a verdict about
/// bytes that are nowhere.
#[tokio::test]
async fn missing_ordinary_file_is_refused_by_the_presence_gate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30320);
    let payload_filename = "silver.horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, file_id) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Missing Ordinary File",
        payload_filename,
        &payload,
    )
    .await;

    // No file is ever written: this job is not direct-store, so nothing excuses
    // the absence.
    assert!(!working_dir.join(payload_filename).exists());
    let par2_set = build_repairable_par2_set(payload_filename, &payload, 64, 1);
    let verification = complete_verification_for(&par2_set, payload_filename);
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();

    assert_eq!(
        report.completed, 0,
        "a file that is not on disk must not be called complete; report = {report:?}"
    );
    assert_eq!(
        report.length_mismatch.len(),
        1,
        "the absence must be reported, not swallowed; report = {report:?}"
    );
    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(file_id)
            .unwrap()
            .is_complete()
    );
}

/// Two descriptions claiming one assembly file bind to neither.
///
/// The mirror of the `by_identity` contest. `or_insert` used to keep whichever
/// description was visited first and drop the other silently, calling the file
/// complete under one of two names with no reason to prefer either.
#[tokio::test]
async fn two_descriptions_claiming_one_file_are_contested() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30321);
    let posted_filename = "silver_horizon.mkv";
    // Sanitizing turns `:` into `_`, so this description answers to the very
    // same name as the one above — two descriptions, one assembly file.
    let colliding_filename = "silver:horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Contested Inverse",
        posted_filename,
        &payload,
    )
    .await;
    tokio::fs::write(working_dir.join(posted_filename), &payload)
        .await
        .unwrap();

    let par2_set = build_repairable_par2_set_for_files(
        &[(posted_filename, &payload), (colliding_filename, &payload)],
        64,
        1,
    );
    let ids: Vec<par2_rs::FileId> = par2_set.recovery_file_ids.clone();
    let verification = par2_rs::VerificationResult {
        files: ids
            .iter()
            .map(|id| par2_rs::verify::FileVerification {
                file_id: *id,
                filename: par2_set.file_description(id).unwrap().filename.clone(),
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            })
            .collect(),
        recovery_blocks_available: 1,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    let report = pipeline
        .reconcile_verified_par2_files(job_id, &verification)
        .await
        .unwrap();

    assert_eq!(
        report.completed, 0,
        "neither claim may promote the file; report = {report:?}"
    );
    assert!(
        !report.contested.is_empty(),
        "the contest must be named; report = {report:?}"
    );
}

/// Accepted repairs shed their leftovers; failed ones keep them.
#[tokio::test]
async fn repair_leftovers_are_purged_only_after_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30322);
    let payload_filename = "silver.horizon.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Repair Leftovers",
        payload_filename,
        &payload,
    )
    .await;
    let par2_filename = "silver.horizon.par2";
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );

    // What the directory held before the repair.
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    // What the repair left behind: the damaged original, renamed aside.
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &payload).await.unwrap();
    // A recovery file the set describes is not a leftover even though it is new.
    tokio::fs::write(working_dir.join(par2_filename), b"par2")
        .await
        .unwrap();

    pipeline.purge_par2_repair_leftovers(job_id);

    assert!(
        !leftover.exists(),
        "an accepted repair must not leave its damaged original to be delivered"
    );
    assert!(
        working_dir.join(payload_filename).exists(),
        "the repaired payload must survive"
    );
    assert!(
        !pipeline.par2_pre_repair_dir_entries.contains_key(&job_id),
        "the snapshot must not survive into the next attempt"
    );

    // Now the failure path: the snapshot is dropped unread, so nothing is
    // deleted and the evidence stays.
    let second_leftover = working_dir.join("silver.horizon.mkv.2");
    tokio::fs::write(&second_leftover, &payload).await.unwrap();
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    pipeline.fail_par2_repair(job_id, "post-repair verification failed".to_string());
    assert!(
        second_leftover.exists(),
        "a failed repair must keep its artefacts for diagnosis"
    );
    assert!(!pipeline.par2_pre_repair_dir_entries.contains_key(&job_id));
}

/// A protected file left incomplete whose verified bytes are nowhere still
/// fails the job; one whose bytes are on disk only warns.
///
/// The invariant frees the job from its article bitmap, not from its bytes. If
/// reconciliation could not promote a protected file *and* the bytes it was
/// vouched for cannot be found, delivering would ship a hole under a
/// verification claiming otherwise — the one case still worth refusing.
#[tokio::test]
async fn protected_defect_fails_only_when_the_bytes_are_gone() {
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let payload_filename = "silver_horizon.mkv";

    // Bytes gone: the verdict vouches for a file that is not there.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30323);
        let (working_dir, _) = incomplete_protected_payload_job(
            &mut pipeline,
            job_id,
            "Protected Bytes Gone",
            payload_filename,
            &payload,
        )
        .await;
        assert!(!working_dir.join(payload_filename).exists());
        let par2_set = build_repairable_par2_set(payload_filename, &payload, 64, 1);
        let verification = complete_verification_for(&par2_set, payload_filename);
        install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

        let error = pipeline
            .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2")
            .await
            .expect_err("a protected file whose bytes are gone must fail the job");
        assert!(
            error.contains("nowhere on disk"),
            "the failure must say the bytes could not be found: {error}"
        );
    }

    // Bytes present, but two descriptions contest the binding: a reconciliation
    // defect over a file that is demonstrably intact. Warn, deliver.
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30324);
        let (working_dir, _) = incomplete_protected_payload_job(
            &mut pipeline,
            job_id,
            "Protected Bytes Present",
            payload_filename,
            &payload,
        )
        .await;
        tokio::fs::write(working_dir.join(payload_filename), &payload)
            .await
            .unwrap();
        let par2_set = build_repairable_par2_set_for_files(
            &[
                (payload_filename, &payload),
                ("silver:horizon.mkv", &payload),
            ],
            64,
            1,
        );
        let ids = par2_set.recovery_file_ids.clone();
        let verification = par2_rs::VerificationResult {
            files: ids
                .iter()
                .map(|id| par2_rs::verify::FileVerification {
                    file_id: *id,
                    filename: par2_set.file_description(id).unwrap().filename.clone(),
                    status: par2_rs::verify::FileStatus::Complete,
                    valid_slices: vec![true, true],
                    missing_slice_count: 0,
                })
                .collect(),
            recovery_blocks_available: 1,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        };
        install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

        pipeline
            .reconcile_and_classify_par2_verification(job_id, &verification, false, "clean PAR2")
            .await
            .expect("a defect over bytes that are demonstrably present must not fail the job");
    }
}

/// A stray on disk is never renamed into a duplicate of a verified file.
///
/// The pre-repair backup par2-rs leaves behind still matches the description
/// over its first 16 KiB, so the renamer offers to move it onto the canonical
/// name. That name is taken by the file the repair just produced, so the
/// allocator used to mint a `.duplicateN` sibling — and the final move, which
/// relocates the whole directory, delivered both copies.
#[tokio::test]
async fn stray_file_is_not_renamed_into_a_duplicate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30325);
    let payload_filename = "silver_horizon.mkv";
    // Job 10000's shape: the damage sits far past the 16 KiB window the
    // renamer matches on, so the leftover still looks exactly like the
    // description it came from.
    let payload: Vec<u8> = (0..65536u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Stray Rename Guard",
        payload_filename,
        &payload,
    )
    .await;

    // The repaired file at its canonical name, and the damaged original the
    // repair renamed aside — `unique_backup_path`'s `.1` suffix, which par2-rs
    // does not count as one of its own generated artifacts.
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    let mut damaged = payload.clone();
    damaged[32768..].fill(0);
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &damaged).await.unwrap();
    assert_eq!(
        par2_rs::checksum::md5(&damaged[..16384]),
        par2_rs::checksum::md5(&payload[..16384]),
        "precondition: the leftover must still match the description's 16 KiB window"
    );

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 1024, 1),
        &[],
    );

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    let entries: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect();
    assert!(
        !entries.iter().any(|name| name.contains("duplicate")),
        "a stray must never become a duplicate of a verified file; entries = {entries:?}"
    );
    assert_eq!(
        std::fs::read(working_dir.join(payload_filename)).unwrap(),
        payload,
        "the canonical file must still be the good one"
    );
}

async fn install_par2_rename_candidate(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posted_filename: &str,
    payload: &[u8],
    described: &[(&str, &[u8])],
) -> PathBuf {
    let (working_dir, _) = incomplete_protected_payload_job(
        pipeline,
        job_id,
        "PAR2 Rename Candidate",
        posted_filename,
        payload,
    )
    .await;
    tokio::fs::write(working_dir.join(posted_filename), payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(described, 1024, 1),
        &[],
    );
    working_dir
}

#[tokio::test]
async fn a_rename_suggestion_with_an_ambiguous_prefix_hash_is_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30326);
    let posted_filename = "a9e3d04c.bin";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 251) as u8).collect();
    let mut alternate = payload.clone();
    alternate[16_384..].fill(0x5a);
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        posted_filename,
        &payload,
        &[
            ("silver-horizon.mkv", &payload),
            ("ivory-meadow.mkv", &alternate),
        ],
    )
    .await;

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(
        working_dir.join(posted_filename).exists(),
        "a shared 16 KiB hash must not choose either target"
    );
    assert!(!working_dir.join("silver-horizon.mkv").exists());
    assert!(!working_dir.join("ivory-meadow.mkv").exists());
}

#[tokio::test]
async fn a_split_fragment_rename_suggestion_is_dropped_before_target_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30327);
    let fragment_filename = "onyx-prairie.mkv.001";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 239) as u8).collect();
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        fragment_filename,
        &payload,
        &[("onyx-prairie.mkv", &payload)],
    )
    .await;
    assert!(!working_dir.join("onyx-prairie.mkv").exists());

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(
        working_dir.join(fragment_filename).exists(),
        "the fragment must remain under its own name"
    );
    assert!(
        !working_dir.join("onyx-prairie.mkv").exists(),
        "the target was free, so keeping it absent proves the suggestion was filtered"
    );
}

#[tokio::test]
async fn a_unique_same_length_obfuscated_rename_still_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30328);
    let posted_filename = "d90f27ac.bin";
    let correct_filename = "silver-horizon.mkv";
    let payload: Vec<u8> = (0..49_152u32).map(|value| (value % 241) as u8).collect();
    let working_dir = install_par2_rename_candidate(
        &mut pipeline,
        job_id,
        posted_filename,
        &payload,
        &[(correct_filename, &payload)],
    )
    .await;

    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    assert!(!working_dir.join(posted_filename).exists());
    assert_eq!(
        std::fs::read(working_dir.join(correct_filename)).unwrap(),
        payload,
        "a unique suggestion with matching length must still deobfuscate"
    );
}

/// A complete, PAR2-protected payload whose bytes are not where their names say.
///
/// `described` gives the recovery set (and the NZB) its file names and the
/// content each name is supposed to hold; `on_disk[i]` is what is actually
/// written at `described[i]`'s name, so a caller expresses a swap by handing the
/// two entries each other's bytes and damage by handing one entry holed bytes.
/// Every article has arrived either way — this is a posting fault, not a
/// download one.
///
/// No archive topology is installed, so the completion gate's integrity gate
/// reads `None` and the job takes the repairer-analysis arm. That is the arm the
/// field job took, and the one that has to tell "nothing to repair, only to
/// place" from "damaged".
async fn misplaced_payload_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    described: &[(&str, Vec<u8>)],
    on_disk: &[Vec<u8>],
    recovery_blocks: usize,
) -> PathBuf {
    assert_eq!(described.len(), on_disk.len());
    let index_filename = "silver-horizon.par2";
    let recovery_filename = "silver-horizon.vol00+01.par2";
    let described_refs: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let par2_bytes = build_test_par2_index_for_files(&described_refs, 64);
    let recovery_bytes = vec![0xAA; 64];

    let mut files: Vec<FileSpec> = described
        .iter()
        .enumerate()
        .map(|(index, (filename, bytes))| FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..bytes.len() as u32 / 64)
                .map(|segment| {
                    segment_spec! {
                        number: segment,
                        bytes: 64,
                        message_id: format!("misplaced-{index}-{segment}@example.com"),
                    }
                })
                .collect(),
        })
        .collect();
    let payload_count = files.len() as u32;
    files.push(FileSpec {
        filename: index_filename.to_string(),
        role: FileRole::from_filename(index_filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: par2_bytes.len() as u32,
            message_id: "misplaced-index@example.com".to_string(),
        }],
    });
    files.push(FileSpec {
        filename: recovery_filename.to_string(),
        role: FileRole::from_filename(recovery_filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: recovery_bytes.len() as u32,
            message_id: "misplaced-recovery@example.com".to_string(),
        }],
    });

    let total_bytes = (described
        .iter()
        .map(|(_, bytes)| bytes.len())
        .sum::<usize>()
        + par2_bytes.len()
        + recovery_bytes.len()) as u64;
    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes,
        category: None,
        metadata: vec![],
        files,
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    for ((filename, _), bytes) in described.iter().zip(on_disk.iter()) {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..payload_count {
            let file_id = NzbFileId { job_id, file_index };
            let file = state.assembly.file_mut(file_id).unwrap();
            let segment_count = described[file_index as usize].1.len() as u32 / 64;
            for segment in 0..segment_count {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }
    write_and_complete_file(pipeline, job_id, payload_count, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        payload_count + 1,
        recovery_filename,
        &recovery_bytes,
    )
    .await;
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(&described_refs, 64, recovery_blocks),
        &[
            (payload_count, index_filename, 0, false),
            (payload_count + 1, recovery_filename, 1, true),
        ],
    );

    working_dir
}

fn misplacement_payload(seed: u32) -> Vec<u8> {
    (0..128u32)
        .map(|value| ((value * 7 + seed * 31) % 251) as u8)
        .collect()
}

/// Two pairs of files posted under each other's names, nothing damaged: the
/// repairer must never be asked to fix a set whose only fault is placement.
///
/// The repairer's own scanner reports every one of them `Renamed`, which makes
/// `needs_repair()` true, and the old ladder read that as "repair required" —
/// with zero damaged slices and zero blocks needed, the tell that there was
/// nothing to repair. Running it anyway installed each file at its canonical
/// name and left the displaced originals behind as `<name>.N`, after which the
/// job's own post-repair pass could no longer tell a backup from the file it had
/// been displaced by. Placement is the whole job here, and the plan for it has
/// to come from a directory scan: a swap is two files each holding the other's
/// content, and only something that looks at what is on each name can see it.
#[tokio::test]
async fn a_placement_only_verdict_is_placed_without_running_the_repairer() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30344);
    let job_name = "Silver Horizon Swapped Parts";
    let described: Vec<(&str, Vec<u8>)> = vec![
        ("silver-horizon-a.bin", misplacement_payload(1)),
        ("silver-horizon-b.bin", misplacement_payload(2)),
        ("silver-horizon-c.bin", misplacement_payload(3)),
        ("silver-horizon-d.bin", misplacement_payload(4)),
    ];
    // a<->b and c<->d, exactly as posted: two physical swaps, four misplaced
    // files, not one damaged byte anywhere.
    let on_disk = vec![
        described[1].1.clone(),
        described[0].1.clone(),
        described[3].1.clone(),
        described[2].1.clone(),
    ];
    let working_dir =
        misplaced_payload_par2_job(&mut pipeline, job_id, job_name, &described, &on_disk, 1).await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_analyze_calls, 1,
        "precondition: this shape reaches the repairer-analysis arm, which is \
         where the misplacement is seen at all"
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 0,
        "a set with nothing damaged has nothing for the repairer to write"
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 1,
        "the analysis is answered with one scanned whole-set pass — the scan is \
         what sees a swap — and that answer stands as the set's verdict"
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        0,
        "and no repair happened, so none may be announced"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    for (filename, bytes) in &described {
        assert_eq!(
            tokio::fs::read(output_dir.join(filename)).await.unwrap(),
            *bytes,
            "{filename} must be delivered holding its own content"
        );
    }
    assert!(
        !working_dir.exists() || std::fs::read_dir(&working_dir).unwrap().next().is_none(),
        "and nothing may be left behind in the working directory"
    );
}

/// Damage alongside misplacement still runs the repairer, and the files it
/// placed are read back where it placed them.
///
/// This is the row the placement-only rule must not swallow: one holed file
/// means slices to reconstruct, so the ladder below it — capacity, promotion,
/// repair — is exactly what the job needs. The renamed files then come back
/// through the post-repair read at their canonical names, because that is where
/// the repair put them.
#[tokio::test]
async fn damage_alongside_misplacement_still_repairs_and_reads_back_the_placed_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30345);
    let job_name = "Silver Horizon Swapped And Holed";
    let described: Vec<(&str, Vec<u8>)> = vec![
        ("silver-horizon-a.bin", misplacement_payload(5)),
        ("silver-horizon-b.bin", misplacement_payload(6)),
        ("silver-horizon-c.bin", misplacement_payload(7)),
    ];
    let mut holed = described[2].1.clone();
    holed[64..].fill(0);
    let on_disk = vec![described[1].1.clone(), described[0].1.clone(), holed];
    let working_dir =
        misplaced_payload_par2_job(&mut pipeline, job_id, job_name, &described, &on_disk, 1).await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "a damaged slice is still a repair"
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        1,
        "and a repair that held is announced"
    );
    let splits = pipeline
        .par2_post_repair_read_splits
        .last()
        .copied()
        .expect("the repair tail ran its post-repair read");
    assert_eq!(
        splits,
        (0usize, 3usize),
        "nothing was complete at its canonical name before the repair, so \
         nothing may be carried: two misplaced files and one holed one all get \
         read back where the repair installed them"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    for (filename, bytes) in &described {
        assert_eq!(
            tokio::fs::read(output_dir.join(filename)).await.unwrap(),
            *bytes,
            "{filename} must be delivered holding its own content"
        );
    }
    assert!(
        !working_dir.exists() || std::fs::read_dir(&working_dir).unwrap().next().is_none(),
        "and the repair's backups may not follow the payload out"
    );
}

/// Stage the identity-rebound misplaced-payload shape as a conventional
/// (non-direct) job and hand back the two pieces a direct quick-verify call
/// needs: the working directory and the served recovery set.
///
/// `described[i]` is the name the recovery set gives file `i` and the content it
/// says that name should hold; `on_disk[i]` is what is actually written at that
/// name — `None` leaves the file absent (never completed). A caller expresses a
/// swap by handing two present entries each other's bytes, damage by handing one
/// holed bytes, and a missing partner by handing `None`. Every present file is
/// completed and its identity rebound to its canonical name with a PAR2 source,
/// which is the post-rebind state the completion gate meets in the field. No
/// article is fed to the dual-CRC grid and no whole-file digest is recorded, so
/// this is the metadata-early shape that streams no MD5 at all — callers that
/// want content evidence add it explicitly.
async fn stage_misplaced_payload_shape(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    described: &[(&str, Vec<u8>)],
    on_disk: &[Option<Vec<u8>>],
) -> (PathBuf, Arc<Par2FileSet>) {
    assert_eq!(described.len(), on_disk.len());
    let files: Vec<(String, u32)> = described
        .iter()
        .zip(on_disk.iter())
        .map(|((name, canonical), disk)| {
            let bytes = disk.as_ref().map_or(canonical.len(), Vec::len);
            ((*name).to_string(), bytes as u32)
        })
        .collect();
    let spec = standalone_job_spec(job_name, &files);
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    for (index, ((name, _), disk)) in described.iter().zip(on_disk.iter()).enumerate() {
        let Some(bytes) = disk else {
            continue;
        };
        write_and_complete_file(pipeline, job_id, index as u32, name, bytes).await;
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index: index as u32,
                    source_filename: (*name).to_string(),
                    current_filename: (*name).to_string(),
                    canonical_filename: Some((*name).to_string()),
                    classification: None,
                    classification_source: crate::jobs::record::FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }

    let described_pairs: Vec<(String, Vec<u8>)> = described
        .iter()
        .map(|(name, canonical)| ((*name).to_string(), canonical.clone()))
        .collect();
    install_test_par2_runtime(
        pipeline,
        job_id,
        placement_par2_file_set(&described_pairs),
        &[],
    );
    let par2_set = Arc::clone(pipeline.par2_set(job_id).expect("served recovery set"));
    (working_dir, par2_set)
}

/// Record a trusted whole-file MD5 for a completed file, the current-generation
/// evidence a metadata-early download deliberately never streams.
fn set_measured_md5(pipeline: &mut Pipeline, job_id: JobId, file_index: u32, content: &[u8]) {
    pipeline
        .ensure_par2_runtime(job_id)
        .completed_checksums
        .insert(
            NzbFileId { job_id, file_index },
            crate::pipeline::CompletedFileChecksum {
                md5: Some(par2_rs::checksum::md5(content)),
                crc32: par2_rs::checksum::crc32(content),
                all_parts_crc_verified: false,
            },
        );
}

/// The field shape: a clean swapped pair under canonical names, downloaded
/// metadata-early, so no whole-file MD5 was ever streamed and no article closed
/// a block on the dual-CRC grid. Quick verify has no evidence keyed to content
/// — only names and lengths — and names are exactly what a swap makes lie, so it
/// must stay inconclusive and leave the authoritative read to decide.
///
/// This is the `no_current_generation_digest` arm: the per-file loop finds
/// neither a closed in-stream block verdict nor a current-generation measured
/// digest, so `measured_md5` is `None` and the loop bails. The companion test
/// `a_misplaced_pair_proven_by_measured_digests_returns_a_swap_plan` shows the
/// identical fixture resolving the swap the moment a digest is present, which is
/// what pins the absence of evidence — not a broken fixture — as the cause here.
#[tokio::test]
async fn a_misplaced_pair_with_no_content_evidence_is_correctly_inconclusive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30360);
    let a = misplacement_payload(1);
    let b = misplacement_payload(2);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap No Evidence",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(a.clone())],
    )
    .await;

    let file0 = NzbFileId {
        job_id,
        file_index: 0,
    };
    let file1 = NzbFileId {
        job_id,
        file_index: 1,
    };
    // Pin the arm. Neither file carries a closed in-stream block verdict ...
    assert!(pipeline.block_crc_verdicts(file0).is_none());
    assert!(pipeline.block_crc_verdicts(file1).is_none());
    // ... nor a current-generation measured digest, in the runtime or the db.
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(!runtime.completed_checksums.contains_key(&file0));
    assert!(!runtime.completed_checksums.contains_key(&file1));
    let persisted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(
        persisted.is_empty(),
        "no persisted digest either: {persisted:?}"
    );

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error on a clean-but-unproven shape");
    assert!(
        result.is_none(),
        "with no digest and no grid verdict the content is unproven, so the \
         quick pass must stay inconclusive and let the authoritative read decide"
    );
    assert_eq!(
        pipeline.par2_quick_verify_calls, 0,
        "an inconclusive pass never counts as a quick verification"
    );
}

/// The same swapped pair, now carrying the trusted whole-file MD5 that a
/// non-metadata-early download would have streamed. The measured digest keys the
/// match to the description its *content* reproduces, not the one its current
/// name implies, so the swap already resolves today: `Ok(Some(..))` with a
/// two-entry swap plan and a clean verdict. This is the control that proves the
/// swap machinery is sound and the inconclusive verdict above is caused solely
/// by absent content evidence.
#[tokio::test]
async fn a_misplaced_pair_proven_by_measured_digests_returns_a_swap_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30361);
    let a = misplacement_payload(3);
    let b = misplacement_payload(4);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap Measured",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(a.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &b);
    set_measured_md5(&mut pipeline, job_id, 1, &a);

    let (verification, plan) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error")
        .expect("measured digests prove the content, so the swap resolves clean");

    assert_eq!(plan.swaps.len(), 1, "the two files are one swap pair");
    assert!(plan.exact.is_empty());
    assert!(plan.renames.is_empty());
    assert!(plan.unresolved.is_empty());
    assert!(plan.conflicts.is_empty());
    assert_eq!(verification.files.len(), 2);
    let (left, right) = &plan.swaps[0];
    let mut correct = [left.correct_name.as_str(), right.correct_name.as_str()];
    correct.sort_unstable();
    assert_eq!(correct, ["silver-horizon-a.bin", "silver-horizon-b.bin"]);
    assert_eq!(pipeline.par2_quick_verify_calls, 1);
}

/// Fail-closed: damage alongside the swap. `silver-horizon-a.bin` holds its
/// partner's clean bytes (a valid swap half), but `silver-horizon-b.bin` holds
/// bytes that reproduce no description's hash. The damaged file matches nothing,
/// its partner description is left unresolved, and the pass refuses — a damaged
/// file must never leave this path with a clean verdict, even with digests
/// present that would otherwise resolve the swap.
#[tokio::test]
async fn a_damaged_file_in_the_misplaced_shape_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30362);
    let a = misplacement_payload(5);
    let b = misplacement_payload(6);
    let mut damaged = b.clone();
    damaged[10] ^= 0xFF; // same length, reproduces no description's hash
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Swap With Damage",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[Some(b.clone()), Some(damaged.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &b);
    set_measured_md5(&mut pipeline, job_id, 1, &damaged);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "a file whose bytes match no description leaves its partner unresolved, \
         so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Fail-closed: one hash claimed by two descriptions. Both descriptions carry
/// the same `hash_full`, so a file matching it is ambiguous — the match count
/// for the chosen id trips two, the id is dropped as a conflict, and its partner
/// description is left unresolved. Ambiguity of this kind is exactly what the
/// authoritative read owns, so the quick pass refuses.
#[tokio::test]
async fn a_hash_claimed_by_two_descriptions_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30363);
    let shared = misplacement_payload(7);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Shared Hash",
        &[
            ("silver-horizon-a.bin", shared.clone()),
            ("silver-horizon-b.bin", shared.clone()),
        ],
        &[Some(shared.clone()), Some(shared.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 0, &shared);
    set_measured_md5(&mut pipeline, job_id, 1, &shared);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "a hash two descriptions answer to is ambiguous, so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Fail-closed: a swap whose partner never arrived. One file is present at
/// `silver-horizon-b.bin`'s canonical name but holds `silver-horizon-a.bin`'s
/// content; the file that should hold the other half never completed. The
/// present half resolves to its content's description, but the absent partner's
/// description has no disk file, so it is left unresolved and the pass refuses.
#[tokio::test]
async fn a_swap_whose_partner_is_absent_is_never_quick_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30364);
    let a = misplacement_payload(11);
    let b = misplacement_payload(12);
    let (working_dir, par2_set) = stage_misplaced_payload_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon Absent Partner",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[None, Some(a.clone())],
    )
    .await;
    set_measured_md5(&mut pipeline, job_id, 1, &a);

    let result = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error");
    assert!(
        result.is_none(),
        "the partner description has no disk file, so the pass must refuse"
    );
    assert_eq!(pipeline.par2_quick_verify_calls, 0);
}

/// Describe a file the set lists but does not protect.
///
/// A PAR2 set's non-recovery files carry a name and the two digests every
/// description carries, and nothing else: no slice checksums, no recovery data.
/// `verify_all` reads only the protected files, while the deobfuscator reads
/// every description — which is what lets a file arrive under a posted name and
/// be given the one the set says it should have.
fn describe_non_recovery_file(
    par2_set: &mut Par2FileSet,
    filename: &str,
    bytes: &[u8],
) -> par2_rs::FileId {
    let length = bytes.len() as u64;
    let hash_full = par2_rs::checksum::md5(bytes);
    let hash_16k = par2_rs::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]);
    let mut id_input = Vec::new();
    id_input.extend_from_slice(&hash_16k);
    id_input.extend_from_slice(&length.to_le_bytes());
    id_input.extend_from_slice(filename.as_bytes());
    let file_id = par2_rs::FileId::from_bytes(par2_rs::checksum::md5(&id_input));
    par2_set.files.insert(
        file_id,
        par2_rs::FileDescription {
            file_id,
            hash_full,
            hash_16k,
            length,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    par2_set.non_recovery_file_ids.push(file_id);
    file_id
}

/// The repair's own leftovers must not fail the repair that produced them.
///
/// par2-rs installs a file at the name its description gives it and moves
/// whatever held that name aside as `<name>.N`. After a swap that leaves the
/// backup holding exactly the content of the file it was displaced by, so a
/// directory scan — the only way a disk file is matched to a description —
/// finds two files for one description and calls the pair a conflict. The
/// settled-layout pass used to be that scan, and it refused accepted repairs
/// over artefacts the repair had just written. It asks the narrower question
/// now: is every described file intact at its canonical name. A leftover cannot
/// answer that one, and it is swept with the rest once the aggregate settles.
#[tokio::test]
async fn repair_leftovers_do_not_fail_the_pass_that_verifies_the_settled_layout() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30346);
    let index_filename = "silver-horizon.par2";
    let posted_notes_filename = "9f2c1a5e.dat";
    let notes_filename = "silver-horizon.nfo";
    let alpha = misplacement_payload(11);
    let beta = misplacement_payload(12);
    let gamma = misplacement_payload(13);
    let notes = misplacement_payload(14);
    let par2_bytes = build_test_par2_index_for_files(
        &[
            ("silver-horizon-a.bin", &alpha),
            ("silver-horizon-b.bin", &beta),
            ("silver-horizon-c.bin", &gamma),
        ],
        64,
    );

    let payload_spec = |index: u32, filename: &str, len: usize| FileSpec {
        filename: filename.to_string(),
        role: FileRole::from_filename(filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: (0..len as u32 / 64)
            .map(|segment| {
                segment_spec! {
                    number: segment,
                    bytes: 64,
                    message_id: format!("leftover-{index}-{segment}@example.com"),
                }
            })
            .collect(),
    };
    let spec = JobSpec {
        name: "Silver Horizon Repair Leftovers".to_string(),
        password: None,
        total_bytes: (alpha.len() + beta.len() + gamma.len() + notes.len() + par2_bytes.len())
            as u64,
        category: None,
        metadata: vec![],
        files: vec![
            payload_spec(0, "silver-horizon-a.bin", alpha.len()),
            payload_spec(1, "silver-horizon-b.bin", beta.len()),
            payload_spec(2, "silver-horizon-c.bin", gamma.len()),
            payload_spec(3, posted_notes_filename, notes.len()),
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "leftover-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // What the repair found, and what it left. Two files were posted under each
    // other's names and one was holed; the repair installed all three at their
    // canonical names, which put the two swapped originals aside as `.1`
    // backups holding each other's content.
    for (filename, bytes) in [
        ("silver-horizon-a.bin", &alpha),
        ("silver-horizon-b.bin", &beta),
        ("silver-horizon-c.bin", &gamma),
        (posted_notes_filename, &notes),
    ] {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..4u32 {
            let file = state
                .assembly
                .file_mut(NzbFileId { job_id, file_index })
                .unwrap();
            for segment in 0..2u32 {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }
    write_and_complete_file(&mut pipeline, job_id, 4, index_filename, &par2_bytes).await;

    let mut par2_set = build_repairable_par2_set_for_files(
        &[
            ("silver-horizon-a.bin", &alpha),
            ("silver-horizon-b.bin", &beta),
            ("silver-horizon-c.bin", &gamma),
        ],
        64,
        1,
    );
    describe_non_recovery_file(&mut par2_set, notes_filename, &notes);
    let alpha_id = par2_set.recovery_file_ids[0];
    let beta_id = par2_set.recovery_file_ids[1];
    let gamma_id = par2_set.recovery_file_ids[2];
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        par2_set,
        &[(4, index_filename, 0, false)],
    );
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();

    // The snapshot the repairer takes on its way in, and the backups it then
    // leaves behind.
    pipeline.par2_pre_repair_dir_entries.insert(
        job_id,
        HashSet::from([
            "silver-horizon-a.bin".to_string(),
            "silver-horizon-b.bin".to_string(),
            "silver-horizon-c.bin".to_string(),
            posted_notes_filename.to_string(),
            index_filename.to_string(),
        ]),
    );
    tokio::fs::write(working_dir.join("silver-horizon-a.bin.1"), &beta)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("silver-horizon-b.bin.1"), &alpha)
        .await
        .unwrap();

    let pre_repair = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: alpha_id,
                filename: "silver-horizon-a.bin".to_string(),
                status: par2_rs::verify::FileStatus::Renamed(
                    working_dir.join("silver-horizon-b.bin"),
                ),
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: beta_id,
                filename: "silver-horizon-b.bin".to_string(),
                status: par2_rs::verify::FileStatus::Renamed(
                    working_dir.join("silver-horizon-a.bin"),
                ),
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: gamma_id,
                filename: "silver-horizon-c.bin".to_string(),
                status: par2_rs::verify::FileStatus::Damaged(1),
                valid_slices: vec![true, false],
                missing_slice_count: 1,
            },
        ],
        recovery_blocks_available: 1,
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 1,
        },
    };
    let outcome = par2_rs::Par2RepairOutcome {
        status: par2_rs::Par2RepairStatus::Repaired,
        files_complete: 3,
        files_renamed: 2,
        files_damaged: 0,
        files_missing: 0,
        available_blocks: 1,
        missing_blocks: 0,
        recovery_blocks_available: 1,
        recovery_blocks_used: 1,
        bytes_copied: (alpha.len() + beta.len()) as u64,
        bytes_reconstructed: 64,
        packets: par2_rs::PacketDiagnostics::default(),
        scan: par2_rs::ScanDiagnostics::default(),
        carry: par2_rs::repairer::CarryDiagnostics::default(),
        verification: pre_repair.clone(),
    };

    pipeline
        .finish_par2_repair(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
            outcome,
            false,
        )
        .await;

    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "the repair held; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        1,
        "a repair whose set is intact where it now sits is a repair that held"
    );
    let mut remaining: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect();
    remaining.sort();
    assert_eq!(
        remaining,
        vec![
            index_filename.to_string(),
            "silver-horizon-a.bin".to_string(),
            "silver-horizon-b.bin".to_string(),
            "silver-horizon-c.bin".to_string(),
            notes_filename.to_string(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>(),
        "exactly the described files and the recovery index survive: the two \
         backups are swept once the aggregate settles, and the posted name the \
         set describes otherwise has been given its own"
    );
    for (filename, bytes) in [
        ("silver-horizon-a.bin", &alpha),
        ("silver-horizon-b.bin", &beta),
        ("silver-horizon-c.bin", &gamma),
        (notes_filename, &notes),
    ] {
        assert_eq!(
            tokio::fs::read(working_dir.join(filename)).await.unwrap(),
            **bytes,
            "{filename} must hold its own content"
        );
    }
}

/// Job 10000 whole: a repaired payload delivered alongside an unprotected file
/// that stayed short of articles.
///
/// Removing the veto stopped this job failing, but a job that cannot fail and
/// cannot finish just spins: the completion gate keeps `has_incomplete_data_files`
/// true forever, re-runs a full authoritative PAR2 pass every couple of seconds,
/// and never reaches finalization. Bounded here so that livelock reads as a
/// failure rather than a hang.
#[tokio::test]
async fn job_with_repaired_payload_and_short_unprotected_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30330);
    let payload_filename = "silver.horizon.mkv";
    let nfo_filename = "silver.horizon.nfo";
    let index_filename = "silver.horizon.par2";
    let recovery_filename = "silver.horizon.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Silver Horizon With Short NFO".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len() + 128)
            as u64,
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
                        message_id: "short-nfo-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "short-nfo-payload-1@example.com".to_string(),
                    },
                ],
            },
            // Not in the recovery set, and one article short: exactly the file
            // that used to fail the job and then used to spin it.
            FileSpec {
                filename: nfo_filename.to_string(),
                role: FileRole::from_filename(nfo_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "short-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "short-nfo-1@example.com".to_string(),
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
                    message_id: "short-nfo-index@example.com".to_string(),
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
                    message_id: "short-nfo-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(nfo_filename), vec![7u8; 64])
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
        // The NFO lost its second article and nothing protects it.
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
    write_and_complete_file(&mut pipeline, job_id, 2, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 3, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (2, index_filename, 0, false),
            (3, recovery_filename, 1, true),
        ],
    );

    for _ in 0..12 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
    }

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a repaired payload must be delivered even though an unprotected file is short; {}",
        debug_job_state(&pipeline, job_id)
    );
}

// ---------------------------------------------------------------------------
// Ignorable "furniture" inside the recovery set
// ---------------------------------------------------------------------------

const FURNITURE_SLICE_SIZE: u64 = 64;

/// A recovery set describing a payload plus one piece of metadata "furniture"
/// (an `.nfo`, an `.sfv`), with the PAR2 index — and, when the set carries
/// recovery slices, one recovery volume — already downloaded.
struct FurnitureJob<'a> {
    name: &'a str,
    payload_filename: &'a str,
    /// The bytes the recovery set describes.
    payload: &'a [u8],
    /// What is on disk under that name, if anything.
    payload_on_disk: Option<&'a [u8]>,
    furniture_filename: &'a str,
    furniture: &'a [u8],
    furniture_on_disk: Option<&'a [u8]>,
    /// Whether the furniture's articles all arrived.
    furniture_articles_complete: bool,
    /// Recovery slices the set carries; also the block count the NZB
    /// advertises for the one recovery volume, which is what the fail-fast
    /// arithmetic reads.
    recovery_blocks: u32,
}

async fn install_furniture_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job: FurnitureJob<'_>,
) -> PathBuf {
    let index_filename = "silver.horizon.par2";
    let recovery_filename = format!("silver.horizon.vol00+{:02}.par2", job.recovery_blocks);
    let described: [(&str, &[u8]); 2] = [
        (job.payload_filename, job.payload),
        (job.furniture_filename, job.furniture),
    ];
    let par2_bytes = build_test_par2_index_for_files(&described, FURNITURE_SLICE_SIZE);
    let recovery_bytes = vec![0xAA; 64];
    let payload_segment_bytes = (job.payload.len() / 2) as u32;

    let mut files = vec![
        FileSpec {
            filename: job.payload_filename.to_string(),
            role: FileRole::from_filename(job.payload_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: payload_segment_bytes,
                    message_id: format!("{}-payload-0@example.com", job.name),
                },
                segment_spec! {
                    number: 1,
                    bytes: payload_segment_bytes,
                    message_id: format!("{}-payload-1@example.com", job.name),
                },
            ],
        },
        FileSpec {
            filename: job.furniture_filename.to_string(),
            role: FileRole::from_filename(job.furniture_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: job.furniture.len() as u32,
                message_id: format!("{}-furniture-0@example.com", job.name),
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
                message_id: format!("{}-index@example.com", job.name),
            }],
        },
    ];
    if job.recovery_blocks > 0 {
        files.push(FileSpec {
            filename: recovery_filename.clone(),
            role: FileRole::from_filename(&recovery_filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: recovery_bytes.len() as u32,
                message_id: format!("{}-recovery@example.com", job.name),
            }],
        });
    }

    let spec = JobSpec {
        name: job.name.to_string(),
        password: None,
        total_bytes: (job.payload.len() + job.furniture.len() + par2_bytes.len() + 128) as u64,
        category: None,
        metadata: vec![],
        files,
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    if let Some(bytes) = job.payload_on_disk {
        tokio::fs::write(working_dir.join(job.payload_filename), bytes)
            .await
            .unwrap();
    }
    if let Some(bytes) = job.furniture_on_disk {
        tokio::fs::write(working_dir.join(job.furniture_filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
        if job.furniture_articles_complete {
            state
                .assembly
                .file_mut(NzbFileId {
                    job_id,
                    file_index: 1,
                })
                .unwrap()
                .commit_segment(0, job.furniture.len() as u32)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 2, index_filename, &par2_bytes).await;
    let mut runtime_files: Vec<(u32, &str, u32, bool)> = vec![(2, index_filename, 0, false)];
    if job.recovery_blocks > 0 {
        write_and_complete_file(pipeline, job_id, 3, &recovery_filename, &recovery_bytes).await;
        runtime_files.push((3, recovery_filename.as_str(), job.recovery_blocks, true));
    }
    install_test_par2_runtime(
        pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &described,
            FURNITURE_SLICE_SIZE,
            job.recovery_blocks as usize,
        ),
        &runtime_files,
    );

    working_dir
}

/// Drive the completion gate until the job settles, the way a live pipeline
/// would through its own re-arms.
async fn settle_job_completion(pipeline: &mut Pipeline, job_id: JobId) {
    for _ in 0..12 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(pipeline).await;
        settle_inflight_moves(pipeline).await;
    }
}

fn intact_furniture_payload() -> Vec<u8> {
    (0..128u32).map(|value| (value % 251) as u8).collect()
}

fn second_half_zeroed(bytes: &[u8]) -> Vec<u8> {
    let mut damaged = bytes.to_vec();
    damaged[64..].fill(0);
    damaged
}

/// A par2-*protected* `.nfo` damaged past what the recovery blocks can rebuild
/// is delivered, not failed.
///
/// Both reference downloaders ship this job: one never raises its
/// "has damaged files" flag for such a file, so the set reports repair-not-
/// needed even when the recovery data said repair was impossible; the other's
/// quick check passes it outright. weaver used to fail the whole download for
/// it, which is the protected sibling of the unprotected-`.nfo` failure.
#[tokio::test]
async fn protected_damaged_ignorable_file_is_delivered_without_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30340);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 97) as u8).collect();
    let damaged_furniture = second_half_zeroed(&furniture);

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Damaged NFO",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&damaged_furniture),
            furniture_articles_complete: true,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "damage confined to furniture must be delivered, not failed; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 0,
        "rebuilding one slice of an .nfo is not worth a full-set read"
    );
}

/// The same rule for a protected `.sfv` that never arrived at all, with no
/// recovery to rebuild it from.
///
/// This is the shape that also has to survive the post-verdict completion gate:
/// the file stays incomplete and bound to a description forever, so counting it
/// as outstanding work would keep the job re-arming instead of finishing.
#[tokio::test]
async fn protected_missing_ignorable_file_is_delivered_without_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30341);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 89) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Missing SFV",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.sfv",
            furniture: &furniture,
            furniture_on_disk: None,
            furniture_articles_complete: false,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a protected .sfv that never posted must not fail the payload; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);
}

/// When something that is *not* furniture is damaged too and the blocks are
/// there, the repairer runs and heals the furniture in the same pass.
///
/// This is the row the spare rule must not swallow: the rebuild is free once
/// the decode matrix is being built anyway, and all three reference behaviours
/// agree on repairing here.
#[tokio::test]
async fn mixed_damage_with_sufficient_blocks_repairs_the_furniture_too() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30342);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 83) as u8).collect();
    let job_name = "Silver Horizon Mixed Repairable";

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: job_name,
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&second_half_zeroed(&payload)),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 2,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "non-furniture damage still repairs, furniture included"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.nfo"))
            .await
            .unwrap(),
        furniture,
        "the furniture is rebuilt in the same pass"
    );
}

/// Mixed damage with the blocks short still fails.
///
/// The furniture's slices cannot be excused out of the solve: a payload file's
/// missing slices are unknowns in every equation the recovery data can form, so
/// sparing the `.nfo` buys the job nothing and delivering a holed payload under
/// a verification that claims otherwise is the one thing that must not happen.
#[tokio::test]
async fn mixed_damage_with_short_blocks_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30343);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 79) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Mixed Short",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&second_half_zeroed(&payload)),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 1,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "damaged payload with short recovery must still fail; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
}

/// The override is the way back to the old rule, and it has to work.
#[tokio::test]
async fn an_empty_ignore_extension_override_restores_the_old_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.par2_ignore_extensions_override = Some(Vec::new());
    let job_id = JobId(30344);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 97) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Override Off",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "an empty override must restore the pre-furniture behaviour; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
}

// ---------------------------------------------------------------------------
// Post-verdict re-entry
// ---------------------------------------------------------------------------

/// A settled PAR2 verdict is never re-derived, and a job that keeps coming back
/// to the gate with a protected file outstanding is reported as the bug it is.
///
/// The state is reachable only through a reconciliation defect of ours — here a
/// contested alias, where two assembly entries answer to one description and
/// the binding is refused rather than guessed. The verified bytes are on disk,
/// so nothing about the download is wrong; what used to happen is that the gate
/// re-read the whole recovery set on every lap, forever, at seconds a lap.
#[tokio::test]
async fn a_settled_par2_verdict_is_not_re_read_for_a_reconciliation_defect() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30345);
    let payload_filename = "silver.horizon.mkv";
    let alias_filename = "silver.horizon.extra.bin";
    let index_filename = "silver.horizon.par2";
    let payload = intact_furniture_payload();
    let par2_bytes = build_test_par2_index(payload_filename, &payload, FURNITURE_SLICE_SIZE);

    let spec = JobSpec {
        name: "Silver Horizon Contested Alias".to_string(),
        password: None,
        total_bytes: (payload.len() * 2 + par2_bytes.len()) as u64,
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
                    bytes: 128,
                    message_id: "contested-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: alias_filename.to_string(),
                role: FileRole::from_filename(alias_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "contested-alias@example.com".to_string(),
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
                    message_id: "contested-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // The verified bytes are on disk under the canonical name; neither assembly
    // entry was ever promoted to complete.
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    write_and_complete_file(&mut pipeline, job_id, 2, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, FURNITURE_SLICE_SIZE, 0),
        &[(2, index_filename, 0, false)],
    );
    // The second entry answers to the same description through a canonical
    // alias, so the description binds to neither.
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 1,
                source_filename: alias_filename.to_string(),
                current_filename: alias_filename.to_string(),
                canonical_filename: Some(payload_filename.to_string()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Declared,
            },
        )
        .expect("recording the contested alias must succeed");
    pipeline.par2_verified.insert(job_id);
    assert!(
        pipeline.incomplete_par2_protected_data_file_count(job_id) > 0,
        "precondition: the gate must still see protected files outstanding"
    );
    pipeline.par2_authoritative_verify_calls = 0;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "a settled verdict must not be re-derived on the retry lap"
    );
    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a reconciliation defect must be reported, not looped on; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("BUG:"),
        "the failure must name itself as ours: {error}"
    );
    assert!(
        error.contains(payload_filename) || error.contains(alias_filename),
        "the failure must name the unbound files: {error}"
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "neither entry may re-read the recovery set"
    );
}

// ---------------------------------------------------------------------------
// Partial recovery volumes
// ---------------------------------------------------------------------------

const PARTIAL_VOLUME_SLICE_SIZE: u64 = 64;
/// Payload slices, and therefore the width of the recovery set's solve.
const PARTIAL_VOLUME_PAYLOAD_SLICES: usize = 8;

struct PartialVolumeJob<'a> {
    name: &'a str,
    /// Leading payload slices zeroed on disk — the damage the repair must cover.
    damaged_slices: usize,
    /// Recovery packets of the short volume whose payload bytes are a hole on
    /// disk. Their headers survive, so only the packet's own MD5 can tell.
    holed_packets: &'a [usize],
}

struct PartialVolumeFixture {
    payload: Vec<u8>,
    short_volume_filename: String,
    short_volume_bytes: Vec<u8>,
    working_dir: PathBuf,
}

/// A job whose damage needs more recovery blocks than the one *complete* volume
/// carries, with the balance sitting in a second volume that lost an article.
///
/// The short volume is on disk with its surviving packets intact and the lost
/// article's bytes zeroed, its assembly entry is one segment short forever, and
/// the recovery set is installed carrying only the complete volume's blocks —
/// so every block the short volume still holds has to be recovered from the
/// bytes themselves or it is not counted at all.
async fn install_partial_volume_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job: PartialVolumeJob<'_>,
) -> PartialVolumeFixture {
    let slice_size = PARTIAL_VOLUME_SLICE_SIZE;
    let slice_bytes = slice_size as usize;
    let payload_filename = "silver.horizon.mkv";
    let index_filename = "silver.horizon.par2";
    let whole_volume_filename = "silver.horizon.vol00+02.par2";
    let short_volume_filename = "silver.horizon.vol02+02.par2";

    let payload: Vec<u8> = (0..(PARTIAL_VOLUME_PAYLOAD_SLICES * slice_bytes) as u32)
        .map(|value| (value % 251) as u8)
        .collect();
    let mut damaged = payload.clone();
    for slice in 0..job.damaged_slices {
        damaged[slice * slice_bytes..(slice + 1) * slice_bytes].fill(0);
    }

    // Four blocks split two-and-two. The set installed below keeps only the
    // first two; the rest must come off the short volume's disk bytes.
    let full_set = build_repairable_par2_set(payload_filename, &payload, slice_size, 4);
    let recovery_set_id = *full_set.recovery_set_id.as_bytes();
    let slice_data = |exponent: u32| -> Vec<u8> {
        full_set.recovery_slices[&exponent]
            .data
            .as_bytes()
            .expect("test recovery slices are built in memory")
            .to_vec()
    };
    let whole_volume_bytes = build_test_par2_recovery_volume(
        recovery_set_id,
        &[(0, &slice_data(0)), (1, &slice_data(1))],
    );
    let mut short_volume_bytes = build_test_par2_recovery_volume(
        recovery_set_id,
        &[(2, &slice_data(2)), (3, &slice_data(3))],
    );
    for packet_index in job.holed_packets {
        punch_recovery_packet_payload(&mut short_volume_bytes, *packet_index, slice_bytes);
    }

    let par2_bytes = build_test_par2_index(payload_filename, &payload, slice_size);
    let payload_segment_bytes = (payload.len() / 2) as u32;
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + slice_bytes;
    // The lost article is the second packet's payload, so the surviving article
    // covers the first packet and the second packet's header.
    let short_head_bytes = (packet_len + par2_rs::packet::header::HEADER_SIZE + 4) as u32;
    let short_tail_bytes = slice_bytes as u32;

    let spec = JobSpec {
        name: job.name.to_string(),
        password: None,
        total_bytes: (payload.len()
            + par2_bytes.len()
            + whole_volume_bytes.len()
            + short_volume_bytes.len()) as u64,
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
                        bytes: payload_segment_bytes,
                        message_id: format!("{}-payload-0@example.com", job.name),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: payload_segment_bytes,
                        message_id: format!("{}-payload-1@example.com", job.name),
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
                    message_id: format!("{}-index@example.com", job.name),
                }],
            },
            FileSpec {
                filename: whole_volume_filename.to_string(),
                role: FileRole::from_filename(whole_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: whole_volume_bytes.len() as u32,
                    message_id: format!("{}-vol00@example.com", job.name),
                }],
            },
            FileSpec {
                filename: short_volume_filename.to_string(),
                role: FileRole::from_filename(short_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: short_head_bytes,
                        message_id: format!("{}-vol02-0@example.com", job.name),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: short_tail_bytes,
                        message_id: format!("{}-vol02-1@example.com", job.name),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        2,
        whole_volume_filename,
        &whole_volume_bytes,
    )
    .await;

    // The short volume: bytes on disk, one article that will never arrive.
    tokio::fs::write(working_dir.join(short_volume_filename), &short_volume_bytes)
        .await
        .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.record_placement(0, 0, short_head_bytes);
        file.commit_segment(0, short_head_bytes).unwrap();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(SegmentId {
            file_id: short_volume_id,
            segment_number: 1,
        });

    let mut installed_set = full_set.clone();
    installed_set.recovery_slices.remove(&2);
    installed_set.recovery_slices.remove(&3);
    install_test_par2_runtime(
        pipeline,
        job_id,
        installed_set,
        &[
            (1, index_filename, 0, false),
            (2, whole_volume_filename, 2, true),
            (3, short_volume_filename, 0, true),
        ],
    );

    PartialVolumeFixture {
        payload,
        short_volume_filename: short_volume_filename.to_string(),
        short_volume_bytes,
        working_dir,
    }
}

/// A recovery volume that lost one article still contributes every block whose
/// packet survived it.
///
/// Recovery merges on file *completion*, so a volume one article short counted
/// zero blocks and the job was failed as unrepairable with its intact packets
/// sitting on disk. Both reference downloaders load such a volume's packets
/// individually — each recovery packet carries its own MD5, which is what makes
/// reading past a hole safe.
#[tokio::test]
async fn a_partial_recovery_volume_contributes_its_surviving_blocks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30350);
    let job_name = "Silver Horizon Partial Volume";

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: job_name,
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        2,
        "precondition: only the complete volume's blocks are counted up front"
    );

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "the surviving packet of the short volume must be counted; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "the salvaged block is what makes the repair possible"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        fixture.payload,
        "the repaired payload must be byte-identical"
    );
}

/// A hole too wide to read past still fails — and the failure counts what was
/// actually salvaged, not what the volume's name advertised.
#[tokio::test]
async fn a_hole_too_wide_to_salvage_past_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30351);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Short",
            // Four damaged slices against two whole blocks plus the one packet
            // the short volume still holds.
            damaged_slices: 4,
            holed_packets: &[1],
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "recovery short even after salvage must still fail; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
    // The count the failure reports, not which of the two shortfall gates
    // rendered it: the salvaged block is part of the arithmetic even when the
    // arithmetic still comes up short.
    assert!(
        error.contains("only 3 recovery blocks"),
        "the failure must count the salvaged block: {error}"
    );
}

/// The salvage reads a short volume once per download generation.
///
/// Nothing about a volume that can no longer complete changes between gate
/// entries, and the gate is entered many times over a job's post-processing —
/// re-reading it on every lap is the shape that turns a slow path into a hot
/// loop.
#[tokio::test]
async fn a_salvaged_recovery_volume_is_read_once_per_generation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30352);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Once",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "first entry salvages the surviving packet; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_recovery_salvage_scans, 1);

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "a second entry must not re-count what it already merged; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_recovery_salvage_scans, 1,
        "a volume that cannot complete is not re-read on every gate entry"
    );
}

/// A volume that completes after a partial salvage reports the whole volume's
/// blocks, not just the ones the completion merge happened to add.
///
/// Regression guard for the accounting the salvage introduces: the completion
/// merge reports *new* slices, which after a salvage is only the remainder.
#[tokio::test]
async fn a_volume_that_completes_after_salvage_reports_its_whole_block_count() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30353);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Refetch",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3
    );

    // The lost article arrives after all: the whole volume lands on disk and the
    // assembly entry completes.
    let mut whole = fixture.short_volume_bytes.clone();
    let slice_bytes = PARTIAL_VOLUME_SLICE_SIZE as usize;
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + slice_bytes;
    let source = build_repairable_par2_set(
        "silver.horizon.mkv",
        &fixture.payload,
        PARTIAL_VOLUME_SLICE_SIZE,
        4,
    );
    let restored = source.recovery_slices[&3]
        .data
        .as_bytes()
        .expect("test recovery slices are built in memory")
        .to_vec();
    let payload_start = packet_len + par2_rs::packet::header::HEADER_SIZE + 4;
    whole[payload_start..payload_start + slice_bytes].copy_from_slice(&restored);
    tokio::fs::write(
        fixture.working_dir.join(&fixture.short_volume_filename),
        &whole,
    )
    .await
    .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.commit_segment(1, slice_bytes as u32).unwrap();
        assert!(file.is_complete());
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .retain(|segment_id| segment_id.file_id != short_volume_id);
    pipeline
        .try_merge_par2_recovery(job_id, short_volume_id)
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        4,
        "the completed volume reports both of its blocks, not just the remainder"
    );
}

/// A volume read back short keeps the count it proved when a later article of
/// it names the whole volume's size.
///
/// Every decoded article registers what its yEnc name claims the volume carries,
/// and for a volume that stranded holding a fraction of that, the claim is a
/// promise nothing can keep. Letting it stand in for the read-back's count told
/// the repair arithmetic the shortfall was already covered, so it asked for
/// nothing further and waited on articles that had already run out.
#[tokio::test]
async fn a_later_articles_yenc_name_does_not_re_credit_a_salvaged_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30354);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Readvertised",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "precondition: the whole volume's two blocks and the one that survived; {}",
        debug_job_state(&pipeline, job_id)
    );

    // The short volume's name advertises two blocks, and the decode path reads
    // that name off every article of it that lands.
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "a stranded volume contributes what it proved, not what it advertises; {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A promoted volume whose segments are parked is waiting, not stranded.
///
/// Parking is where a promoted file's work rests between the promotion and the
/// completion gate that hands it back to the download queue. Reading "nothing in
/// flight" as "cannot complete" makes that window look terminal, and the volume
/// is read back for the fraction of itself that happens to be on disk.
#[tokio::test]
async fn a_promoted_volume_whose_segments_are_parked_is_not_read_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30355);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Parked",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    let missing_segment = SegmentId {
        file_id: short_volume_id,
        segment_number: 1,
    };

    // The fixture's article has run out of servers. Put it back in the state it
    // passes through first: parked, waiting for the gate to promote it.
    pipeline
        .unavailable_promoted_recovery_segments
        .retain(|segment_id| *segment_id != missing_segment);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue.push(DownloadWork {
            segment_id: missing_segment,
            message_id: MessageId::new("silver-horizon-parked-vol02-1@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: PARTIAL_VOLUME_SLICE_SIZE as u32,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        0,
        "a volume whose work is parked has not finished arriving; {}",
        debug_job_state(&pipeline, job_id)
    );

    // Now the article really is gone.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue.drain_all();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(missing_segment);

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        1,
        "a volume that can no longer complete is read back; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "{}",
        debug_job_state(&pipeline, job_id)
    );
}

/// Recovery packets of a short volume posted one article per packet.
///
/// The two-article fixture above cannot express a volume that takes *more* bytes
/// and is still short: its only outstanding article is the one that would
/// complete it. This one loses its middle article and keeps its last, so the
/// volume grows on disk twice while never completing.
struct GrowingVolumeFixture {
    working_dir: PathBuf,
    volume_filename: String,
    /// The volume's packets in posting order, one per article.
    packets: Vec<Vec<u8>>,
}

impl GrowingVolumeFixture {
    fn packet_len(&self) -> usize {
        self.packets[0].len()
    }

    /// The volume as it looks on disk once `arrived` articles have landed: the
    /// packets that came, and holes where the rest will go.
    fn on_disk(&self, arrived: &[usize]) -> Vec<u8> {
        let mut bytes = vec![0u8; self.packets.len() * self.packet_len()];
        for index in arrived {
            let start = index * self.packet_len();
            bytes[start..start + self.packet_len()].copy_from_slice(&self.packets[*index]);
        }
        bytes
    }
}

async fn install_growing_partial_volume_par2_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
) -> GrowingVolumeFixture {
    let slice_size = PARTIAL_VOLUME_SLICE_SIZE;
    let slice_bytes = slice_size as usize;
    let payload_filename = "ivory.meadow.mkv";
    let index_filename = "ivory.meadow.par2";
    let whole_volume_filename = "ivory.meadow.vol00+01.par2";
    let short_volume_filename = "ivory.meadow.vol01+03.par2";

    let payload: Vec<u8> = (0..(PARTIAL_VOLUME_PAYLOAD_SLICES * slice_bytes) as u32)
        .map(|value| (value % 241) as u8)
        .collect();
    let full_set = build_repairable_par2_set(payload_filename, &payload, slice_size, 4);
    let recovery_set_id = *full_set.recovery_set_id.as_bytes();
    let slice_data = |exponent: u32| -> Vec<u8> {
        full_set.recovery_slices[&exponent]
            .data
            .as_bytes()
            .expect("test recovery slices are built in memory")
            .to_vec()
    };
    let whole_volume_bytes =
        build_test_par2_recovery_volume(recovery_set_id, &[(0, &slice_data(0))]);
    // One packet per article, so the volume can take an article without being
    // finished by it.
    let packets: Vec<Vec<u8>> = (1..4u32)
        .map(|exponent| {
            build_test_par2_recovery_volume(recovery_set_id, &[(exponent, &slice_data(exponent))])
        })
        .collect();
    let packet_len = packets[0].len();

    let par2_bytes = build_test_par2_index(payload_filename, &payload, slice_size);
    let payload_segment_bytes = (payload.len() / 2) as u32;

    let spec = JobSpec {
        name: "Ivory Meadow Growing Volume".to_string(),
        password: None,
        total_bytes: (payload.len()
            + par2_bytes.len()
            + whole_volume_bytes.len()
            + packets.len() * packet_len) as u64,
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
                        bytes: payload_segment_bytes,
                        message_id: "ivory-meadow-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: payload_segment_bytes,
                        message_id: "ivory-meadow-payload-1@example.com".to_string(),
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
                    message_id: "ivory-meadow-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: whole_volume_filename.to_string(),
                role: FileRole::from_filename(whole_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: whole_volume_bytes.len() as u32,
                    message_id: "ivory-meadow-vol00@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: short_volume_filename.to_string(),
                role: FileRole::from_filename(short_volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..3u32)
                    .map(|ordinal| {
                        segment_spec! {
                            number: ordinal,
                            bytes: packet_len as u32,
                            message_id: format!("ivory-meadow-vol01-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let payload_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        for segment_number in 0..2 {
            state
                .assembly
                .file_mut(payload_id)
                .unwrap()
                .commit_segment(segment_number, payload_segment_bytes)
                .unwrap();
        }
    }

    write_and_complete_file(pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(
        pipeline,
        job_id,
        2,
        whole_volume_filename,
        &whole_volume_bytes,
    )
    .await;

    let fixture = GrowingVolumeFixture {
        working_dir,
        volume_filename: short_volume_filename.to_string(),
        packets,
    };

    // Only the volume's first article has landed; its second has run out of
    // servers, and its third has yet to arrive.
    tokio::fs::write(
        fixture.working_dir.join(&fixture.volume_filename),
        fixture.on_disk(&[0]),
    )
    .await
    .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.record_placement(0, 0, packet_len as u32);
        file.commit_segment(0, packet_len as u32).unwrap();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(SegmentId {
            file_id: short_volume_id,
            segment_number: 1,
        });

    let mut installed_set = full_set.clone();
    for exponent in 1..4u32 {
        installed_set.recovery_slices.remove(&exponent);
    }
    install_test_par2_runtime(
        pipeline,
        job_id,
        installed_set,
        &[
            (1, index_filename, 0, false),
            (2, whole_volume_filename, 1, true),
            (3, short_volume_filename, 0, true),
        ],
    );

    fixture
}

/// A volume read back short is read again once more of it lands.
///
/// One read-back per *generation of bytes*, not one ever. A volume can strand,
/// be read for what it holds, take another article and strand again — and the
/// second stranding has more on disk than the first read saw. Latching the
/// read-back to the file for good left those blocks unaccounted for the rest of
/// the job, because the latch is only ever cleared by a completion the volume
/// will never reach.
#[tokio::test]
async fn a_volume_read_back_short_is_read_again_once_more_of_it_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30356);

    let fixture = install_growing_partial_volume_par2_job(&mut pipeline, job_id).await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(pipeline.par2_recovery_salvage_scans, 1);
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        2,
        "the whole volume's block and the one article of the short volume; {}",
        debug_job_state(&pipeline, job_id)
    );

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans, 1,
        "nothing about the volume has moved, so it is not read again"
    );

    // The volume's last article lands. It still cannot complete — the middle one
    // has run out of servers — but there is more on disk than the first read saw.
    tokio::fs::write(
        fixture.working_dir.join(&fixture.volume_filename),
        fixture.on_disk(&[0, 2]),
    )
    .await
    .unwrap();
    let packet_len = fixture.packet_len() as u32;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 3,
            })
            .unwrap();
        file.record_placement(2, 2 * packet_len as u64, packet_len);
        file.commit_segment(2, packet_len).unwrap();
        assert!(!file.is_complete());
    }

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        2,
        "more bytes landed, so the volume is read again; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "the second read-back adds to the first rather than replacing it; {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A repair short of blocks that nothing can still deliver is failed, not
/// waited on.
///
/// The wait branch fails only when *capacity* is short. A job whose targeted
/// total already covers the damage promotes nothing, and with nothing queued,
/// active, retrying or decoding there is no arrival that could raise the
/// available count — the one pass that could, the read-back, ran on the way in.
/// The branch nonetheless moved the job to `Downloading` and returned, forever.
#[tokio::test]
async fn a_repair_waiting_on_recovery_that_cannot_arrive_is_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30357);
    let payload_filename = "onyx.prairie.mkv";
    let index_filename = "onyx.prairie.par2";
    // A volume that arrived whole and whose bytes yielded no packet at all: its
    // name is the only thing that ever said how much recovery it carries.
    let recovery_filename = "onyx.prairie.vol00+02.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| ((value * 7) % 251) as u8).collect();
    let damaged_payload = vec![0u8; original_payload.len()];
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0x55u8; 64];
    let spec = JobSpec {
        name: "Onyx Prairie Unreachable Recovery".to_string(),
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
                        message_id: "onyx-prairie-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "onyx-prairie-payload-1@example.com".to_string(),
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
                    message_id: "onyx-prairie-index@example.com".to_string(),
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
                    message_id: "onyx-prairie-recovery@example.com".to_string(),
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
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 2, true),
        ],
    );

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a wait no arrival can end must be a failure; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("no further recovery can arrive"),
        "the failure must say why waiting is pointless: {error}"
    );
}

/// The state the hung job reached, end to end: one volume complete, one
/// stranded holding a fraction of what it advertises, and nothing left in
/// flight.
#[tokio::test]
async fn a_stranded_volume_advertising_more_than_it_holds_fails_instead_of_waiting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30358);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Stall",
            damaged_slices: 4,
            holed_packets: &[1],
        },
    )
    .await;

    // The order the hung job reached it in: the volume is read back for what it
    // holds, and only afterwards does an article of it decode and register what
    // its name claims.
    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a repair that can never afford its damage must be failed, not waited on; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("only 3 recovery blocks"),
        "the failure must count what the volume proved: {error}"
    );
}

/// The same shape with the damage inside what the read-back recovered: the
/// repair runs.
///
/// Guard for the failure above — counting a stranded volume honestly must not
/// turn a job that can afford its damage into a failure.
#[tokio::test]
async fn a_stranded_volume_whose_read_back_covers_the_damage_still_repairs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30359);
    let job_name = "Silver Horizon Partial Volume Stall Averted";

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: job_name,
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        fixture.payload,
        "the repaired payload must be byte-identical"
    );
}

// ---------------------------------------------------------------------------
// Postings carrying more than one recovery set
// ---------------------------------------------------------------------------

const TWO_SET_SLICE_SIZE: u64 = 64;
const LARGER_PAYLOAD: &str = "silver.horizon.mkv";
const LARGER_INDEX: &str = "silver.horizon.par2";
const LARGER_VOLUME: &str = "silver.horizon.vol00+08.par2";
const SMALLER_PAYLOAD: &str = "amber.trail.mkv";
const SMALLER_INDEX: &str = "amber.trail.par2";
const SMALLER_VOLUME: &str = "amber.trail.vol00+04.par2";

/// One posting carrying two independent recovery sets.
///
/// The sets describe different files and share no bytes, and one protects four
/// times the payload of the other — so which of them is served has to be a
/// decision rather than an accident of arrival order. File indices are fixed
/// so a test can name them: 0/1/2 are the larger set's payload, index and
/// volume, 3/4/5 the smaller set's.
struct TwoSetPosting {
    larger_payload: Vec<u8>,
    larger_index: Vec<u8>,
    larger_volume: Vec<u8>,
    smaller_payload: Vec<u8>,
    smaller_index: Vec<u8>,
    smaller_volume: Vec<u8>,
}

impl TwoSetPosting {
    fn build() -> Self {
        let larger_payload: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
        let smaller_payload: Vec<u8> = (0..128u32).map(|value| (value % 241) as u8).collect();
        let larger_index = build_test_par2_index_for_files(
            &[(LARGER_PAYLOAD, &larger_payload)],
            TWO_SET_SLICE_SIZE,
        );
        let smaller_index = build_test_par2_index_for_files(
            &[(SMALLER_PAYLOAD, &smaller_payload)],
            TWO_SET_SLICE_SIZE,
        );
        let recovery_slice = |fill: u8| vec![fill; TWO_SET_SLICE_SIZE as usize];
        let larger_slices: Vec<Vec<u8>> =
            (0..8u8).map(|index| recovery_slice(0xB0 + index)).collect();
        let smaller_slices: Vec<Vec<u8>> =
            (0..4u8).map(|index| recovery_slice(0xA0 + index)).collect();
        let larger_volume = build_test_par2_recovery_volume(
            *Self::recovery_set_id(&larger_index).as_bytes(),
            &larger_slices
                .iter()
                .enumerate()
                .map(|(exponent, slice)| (exponent as u32, slice.as_slice()))
                .collect::<Vec<_>>(),
        );
        let smaller_volume = build_test_par2_recovery_volume(
            *Self::recovery_set_id(&smaller_index).as_bytes(),
            &smaller_slices
                .iter()
                .enumerate()
                .map(|(exponent, slice)| (exponent as u32, slice.as_slice()))
                .collect::<Vec<_>>(),
        );
        Self {
            larger_payload,
            larger_index,
            larger_volume,
            smaller_payload,
            smaller_index,
            smaller_volume,
        }
    }

    fn recovery_set_id(par2_bytes: &[u8]) -> par2_rs::RecoverySetId {
        par2_rs::Par2FileSet::from_files(&[par2_bytes])
            .expect("the fixture index must parse")
            .recovery_set_id
    }

    fn spec(&self) -> JobSpec {
        let payload_segment = (self.larger_payload.len() / 2) as u32;
        let smaller_segment = (self.smaller_payload.len() / 2) as u32;
        JobSpec {
            name: "Two Recovery Sets".to_string(),
            password: None,
            total_bytes: (self.larger_payload.len() + self.smaller_payload.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: LARGER_PAYLOAD.to_string(),
                    role: FileRole::from_filename(LARGER_PAYLOAD),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![
                        segment_spec! {
                            number: 0,
                            bytes: payload_segment,
                            message_id: "two-sets-larger-0@example.com".to_string(),
                        },
                        segment_spec! {
                            number: 1,
                            bytes: payload_segment,
                            message_id: "two-sets-larger-1@example.com".to_string(),
                        },
                    ],
                },
                FileSpec {
                    filename: LARGER_INDEX.to_string(),
                    role: FileRole::from_filename(LARGER_INDEX),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.larger_index.len() as u32,
                        message_id: "two-sets-larger-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: LARGER_VOLUME.to_string(),
                    role: FileRole::from_filename(LARGER_VOLUME),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.larger_volume.len() as u32,
                        message_id: "two-sets-larger-volume@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: SMALLER_PAYLOAD.to_string(),
                    role: FileRole::from_filename(SMALLER_PAYLOAD),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![
                        segment_spec! {
                            number: 0,
                            bytes: smaller_segment,
                            message_id: "two-sets-smaller-0@example.com".to_string(),
                        },
                        segment_spec! {
                            number: 1,
                            bytes: smaller_segment,
                            message_id: "two-sets-smaller-1@example.com".to_string(),
                        },
                    ],
                },
                FileSpec {
                    filename: SMALLER_INDEX.to_string(),
                    role: FileRole::from_filename(SMALLER_INDEX),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.smaller_index.len() as u32,
                        message_id: "two-sets-smaller-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: SMALLER_VOLUME.to_string(),
                    role: FileRole::from_filename(SMALLER_VOLUME),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: self.smaller_volume.len() as u32,
                        message_id: "two-sets-smaller-volume@example.com".to_string(),
                    }],
                },
            ],
        }
    }

    /// Seed the job and land both index files, exactly as the download path
    /// does — an index is parsed because it finished arriving — without
    /// parsing either yet.
    async fn install(&self, pipeline: &mut Pipeline, job_id: JobId) -> PathBuf {
        let working_dir = insert_active_job(pipeline, job_id, self.spec()).await;
        write_and_complete_file(pipeline, job_id, 1, LARGER_INDEX, &self.larger_index).await;
        write_and_complete_file(pipeline, job_id, 4, SMALLER_INDEX, &self.smaller_index).await;
        working_dir
    }
}

async fn load_par2_index(pipeline: &mut Pipeline, job_id: JobId, file_index: u32) {
    pipeline
        .try_load_par2_metadata(job_id, NzbFileId { job_id, file_index })
        .await;
}

fn served_set_describes(pipeline: &Pipeline, job_id: JobId, filename: &str) -> bool {
    pipeline
        .par2_set(job_id)
        .is_some_and(|set| set.files.values().any(|desc| desc.filename == filename))
}

/// Whichever index lands first, the set protecting the most payload is served.
///
/// Before this, the last index to be parsed simply replaced the set — so a live
/// job served whichever index finished downloading last, and the same job
/// replayed from disk served whichever came last in the posting. The two need
/// not be the same set, which makes a repair's outcome depend on arrival order.
#[tokio::test]
async fn the_larger_recovery_set_is_served_whichever_index_lands_first() {
    let posting = TwoSetPosting::build();
    let expected = TwoSetPosting::recovery_set_id(&posting.larger_index);

    let larger_first_dir = tempfile::tempdir().unwrap();
    let (mut larger_first, _, _) = new_direct_pipeline(&larger_first_dir).await;
    let job_id = JobId(30801);
    posting.install(&mut larger_first, job_id).await;
    load_par2_index(&mut larger_first, job_id, 1).await;
    load_par2_index(&mut larger_first, job_id, 4).await;

    let smaller_first_dir = tempfile::tempdir().unwrap();
    let (mut smaller_first, _, _) = new_direct_pipeline(&smaller_first_dir).await;
    posting.install(&mut smaller_first, job_id).await;
    load_par2_index(&mut smaller_first, job_id, 4).await;
    load_par2_index(&mut smaller_first, job_id, 1).await;

    let larger_first_id = larger_first
        .par2_set(job_id)
        .expect("a set must be served")
        .recovery_set_id;
    let smaller_first_id = smaller_first
        .par2_set(job_id)
        .expect("a set must be served")
        .recovery_set_id;

    assert_eq!(
        larger_first_id, smaller_first_id,
        "arrival order must not decide which recovery set a job serves"
    );
    assert_eq!(
        larger_first_id, expected,
        "the set protecting the most payload is the one worth serving"
    );
    assert!(
        served_set_describes(&smaller_first, job_id, LARGER_PAYLOAD),
        "the served set must describe the larger payload"
    );
    assert!(
        !served_set_describes(&smaller_first, job_id, SMALLER_PAYLOAD),
        "the unserved set's file must not appear in the served set"
    );
}

/// The other set's recovery volumes are not this repair's recovery blocks.
///
/// They repair the other set's files, so counting them advertises capacity for
/// a repair they can take no part in — and sends the fetcher after blocks that
/// will never help.
#[tokio::test]
async fn another_recovery_sets_volumes_do_not_count_toward_the_served_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30802);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;

    assert_eq!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap()),
        8,
        "only the served set's volume advertises capacity for this repair"
    );

    // The other set's volume lands whole. Its packets are real, they validate,
    // and every one of them answers to the set this job does not serve.
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        0,
        "a completed volume of another set contributes nothing to this repair"
    );
}

/// A file can retain known coverage when its set's index never arrives.
///
/// A foreign packet identifies the recovery set, but without an index no pass
/// can verify or repair its files. That remains distinct from an unprotected
/// file: delivery is the same, while the diagnostic explains why no recovery
/// action was possible.
#[tokio::test]
async fn a_file_of_an_unservable_recovery_set_is_not_reported_as_unprotected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30803);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    let unservable = pipeline
        .ensure_par2_runtime(job_id)
        .set_runtime_mut(smaller_set_id)
        .expect("foreign packets must retain their recovery set");
    assert!(unservable.set.is_none());
    unservable
        .summary
        .described_filenames
        .push(SMALLER_PAYLOAD.to_string());

    tokio::fs::write(working_dir.join(LARGER_PAYLOAD), &posting.larger_payload)
        .await
        .unwrap();
    tokio::fs::write(
        working_dir.join(SMALLER_PAYLOAD),
        &posting.smaller_payload[..64],
    )
    .await
    .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for (file_index, committed) in [(0u32, 2u32), (3, 1)] {
            let file = state
                .assembly
                .file_mut(NzbFileId { job_id, file_index })
                .unwrap();
            for segment in 0..committed {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }

    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "two recovery sets",
        )
        .expect("the file left short must still be reported");

    assert!(
        report.message.contains("no posted index"),
        "the report must say the file's known set cannot receive a pass: {}",
        report.message
    );
    assert!(
        report.message.contains(SMALLER_PAYLOAD),
        "the report must name the file that came up short: {}",
        report.message
    );
    assert!(
        !report.message.contains("unprotected"),
        "a file a set still covers was never unprotected: {}",
        report.message
    );
    assert_eq!(
        report.unproven_protected, 0,
        "a file of an unservable set is not a reconciliation defect: {}",
        report.message
    );
    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "nothing a servable set can act on is left, so the gate must not re-arm"
    );
}

/// The announcement is worth exactly one line per job for indexless sets.
///
/// The completion gate is entered many times and no index appears between
/// entries, so repeating the warning would add noise without new information.
#[tokio::test]
async fn an_unservable_recovery_set_is_announced_once_per_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30804);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    pipeline.warn_unservable_recovery_sets_once(job_id);

    assert_eq!(
        pipeline.par2_unserved_set_warnings, 1,
        "meeting the second set is what there is to say"
    );

    for _ in 0..3 {
        pipeline.warn_unservable_recovery_sets_once(job_id);
    }
    assert_eq!(
        pipeline.par2_unserved_set_warnings, 1,
        "re-entering the gate must not repeat it"
    );
}

/// The ordinary single-set posting is untouched by any of this.
#[tokio::test]
async fn a_single_recovery_set_job_announces_nothing_and_counts_every_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30805);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;

    assert_eq!(
        pipeline.par2_unserved_set_warnings, 0,
        "one set is not a multi-set posting"
    );
    assert_eq!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap()),
        12,
        "with one set known, every recovery volume in the posting still counts"
    );
}

#[tokio::test]
async fn targeted_promotion_routes_only_to_the_requested_recovery_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30807);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    assert_eq!(
        pipeline.promote_recovery_targeted(job_id, smaller_set_id, 4),
        4,
        "the requested set's smallest volume covers all four requested blocks"
    );
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(
        runtime.files[&5].promoted,
        "the requested set's volume was promoted"
    );
    assert!(
        !runtime.files.get(&2).is_some_and(|file| file.promoted),
        "the other set's volume stayed parked"
    );
}

#[tokio::test]
async fn a_volume_completed_before_its_index_is_replayed_into_that_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30808);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(smaller_set_id)
            .is_some_and(|set_runtime| set_runtime.set.is_none()),
        "the volume can identify its set before that set has an index"
    );

    load_par2_index(&mut pipeline, job_id, 4).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, smaller_set_id),
        4,
        "installing the index replays the already-complete volume into its own set"
    );
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4,
        "replay must feed the set itself, not only filename-derived arithmetic"
    );
}

#[tokio::test]
async fn recovery_arithmetic_is_strictly_isolated_per_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30809);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        8
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        4
    );

    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, larger_set_id),
        8
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, smaller_set_id),
        4
    );
}

#[tokio::test]
async fn a_multi_set_recovery_file_feeds_both_sets_and_counts_for_each() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30810);
    let posting = TwoSetPosting::build();
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + TWO_SET_SLICE_SIZE as usize;
    let mut multi_set_volume = posting.larger_volume[..packet_len].to_vec();
    multi_set_volume.extend_from_slice(&posting.smaller_volume[..packet_len]);
    let mut spec = posting.spec();
    spec.files[5].segments[0].bytes = multi_set_volume.len() as u32;
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        LARGER_INDEX,
        &posting.larger_index,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        4,
        SMALLER_INDEX,
        &posting.smaller_index,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    write_and_complete_file(&mut pipeline, job_id, 5, SMALLER_VOLUME, &multi_set_volume).await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert_eq!(runtime.files[&5].recovery_set_id, None);
    assert!(runtime.files[&5].recovery_set_packets_read);
    assert_eq!(
        runtime
            .set_runtime(larger_set_id)
            .unwrap()
            .set
            .as_ref()
            .unwrap()
            .recovery_block_count(),
        1
    );
    assert_eq!(
        runtime
            .set_runtime(smaller_set_id)
            .unwrap()
            .set
            .as_ref()
            .unwrap()
            .recovery_block_count(),
        1
    );
    // The blocks are merged into both sets and the repairer counts them, so
    // the arithmetic that decides whether a repair is affordable has to see
    // them too. Attributing the *file* to neither set is still right — no one
    // set owns it — but that is a question about ownership, not about how much
    // recovery each set actually holds.
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        9,
        "the larger set's own eight blocks plus the one this file gave it"
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        1,
        "and the smaller set sees the block it was actually given"
    );
}

#[tokio::test]
async fn a_second_index_of_an_unserved_set_merges_its_packets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30811);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    let mut second_index = posting.smaller_index.clone();
    second_index.extend_from_slice(&posting.smaller_volume);
    tokio::fs::write(working_dir.join(SMALLER_INDEX), second_index)
        .await
        .unwrap();

    load_par2_index(&mut pipeline, job_id, 4).await;
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4,
        "the later index augments the non-served set instead of replacing it"
    );
}

#[tokio::test]
async fn restart_replay_rebuilds_every_set_and_its_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30812);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    pipeline.restore_par2_state_from_disk(job_id).await;

    assert_eq!(
        pipeline
            .par2_set_for(job_id, larger_set_id)
            .unwrap()
            .recovery_block_count(),
        8
    );
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4
    );
}

const VOLUME_BOOTSTRAP_PAYLOAD: &str = "copper.aurora.bin";
const VOLUME_BOOTSTRAP_INDEX: &str = "copper.aurora.par2";
const VOLUME_BOOTSTRAP_VOLUME: &str = "copper.aurora.vol00+01.par2";

fn volume_only_par2_bootstrap_fixture() -> (JobSpec, Vec<u8>, par2_rs::RecoverySetId) {
    let payload = b"copper aurora payload";
    let metadata = build_test_par2_index(VOLUME_BOOTSTRAP_PAYLOAD, payload, 64);
    let recovery_set_id = par2_rs::Par2FileSet::from_files(&[&metadata])
        .expect("fixture metadata must parse")
        .recovery_set_id;
    let mut volume = metadata;
    volume.extend_from_slice(&build_test_par2_recovery_volume(
        *recovery_set_id.as_bytes(),
        &[(0, &[0xC3; 64])],
    ));
    let spec = JobSpec {
        name: "Volume Metadata Bootstrap".to_string(),
        password: None,
        total_bytes: (payload.len() + volume.len() + 1) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: VOLUME_BOOTSTRAP_PAYLOAD.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "volume-bootstrap-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: VOLUME_BOOTSTRAP_INDEX.to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 1,
                    message_id: "volume-bootstrap-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: VOLUME_BOOTSTRAP_VOLUME.to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: volume.len() as u32,
                    message_id: "volume-bootstrap-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    (spec, volume, recovery_set_id)
}

#[tokio::test]
async fn a_complete_volume_bootstraps_its_recovery_set_without_its_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30815);
    let (spec, volume, recovery_set_id) = volume_only_par2_bootstrap_fixture();
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 2, VOLUME_BOOTSTRAP_VOLUME, &volume).await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .await;

    let set = pipeline
        .par2_set_for(job_id, recovery_set_id)
        .expect("the complete volume carries enough metadata to establish its set");
    assert!(
        set.files
            .values()
            .any(|file| file.filename == VOLUME_BOOTSTRAP_PAYLOAD),
        "the volume-built set retains its file descriptions"
    );
    assert_eq!(set.recovery_block_count(), 1);
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, recovery_set_id),
        1,
        "the volume's recovery block is available to its newly established set"
    );
}

#[tokio::test]
async fn restart_replay_bootstraps_a_set_from_its_only_complete_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30816);
    let (spec, volume, recovery_set_id) = volume_only_par2_bootstrap_fixture();
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 2, VOLUME_BOOTSTRAP_VOLUME, &volume).await;
    pipeline.restore_par2_state_from_disk(job_id).await;

    let set = pipeline
        .par2_set_for(job_id, recovery_set_id)
        .expect("restart replay must rebuild a set from the surviving volume");
    assert!(
        set.files
            .values()
            .any(|file| file.filename == VOLUME_BOOTSTRAP_PAYLOAD)
    );
    assert_eq!(set.recovery_block_count(), 1);
}

#[test]
fn retained_sessions_evict_the_oldest_unprotected_job_and_set_pair() {
    let job_id = JobId(30813);
    let older = (job_id, par2_rs::RecoverySetId::from_bytes([1; 16]));
    let protected = (job_id, par2_rs::RecoverySetId::from_bytes([2; 16]));
    let now = std::time::Instant::now();

    assert_eq!(
        crate::pipeline::repair::par2::select_par2_session_eviction(
            [
                (older, true, Some(now - std::time::Duration::from_secs(2))),
                (
                    protected,
                    true,
                    Some(now - std::time::Duration::from_secs(1))
                ),
            ],
            protected,
        ),
        Some(older),
        "the unprotected set of the same job remains eligible for LRU eviction"
    );
}

#[tokio::test]
async fn a_single_set_keeps_capacity_promotion_salvage_and_sessions_available() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(true);
    let job_id = JobId(30814);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);

    assert_eq!(pipeline.total_recovery_block_capacity(job_id, set_id), 12);
    assert_eq!(pipeline.promote_recovery_targeted(job_id, set_id, 4), 4);
    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    let session = pipeline
        .take_or_open_par2_repair_session(job_id, set_id, working_dir, 8 * 1024 * 1024, None, None)
        .await
        .unwrap()
        .expect("the single set has an index path for a retained session")
        .0;
    pipeline.restore_par2_repair_session(job_id, set_id, session);
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .is_some_and(|set_runtime| set_runtime.session.is_some())
    );
}

// ---------------------------------------------------------------------------
// A recovery set that describes the joined file, against a posting of parts
// ---------------------------------------------------------------------------

/// The name the recovery set speaks for. Nothing in the posting is called this:
/// the parts join into it.
const SPLIT_JOIN_JOINED_FILENAME: &str = "Ivory.Meadow.mkv";
/// A recovery volume in the posting, so the fail arithmetic has capacity to
/// spend before the repairer is ever asked for a verdict.
const SPLIT_JOIN_RECOVERY_FILENAME: &str = "Ivory.Meadow.mkv.vol00+02.par2";
const SPLIT_JOIN_RECOVERY_BLOCKS: u32 = 2;

fn split_join_payload(len: usize) -> Vec<u8> {
    (0..len).map(|value| (value % 251) as u8).collect()
}

/// One posted part of a plain split set.
struct SplitJoinPart {
    filename: String,
    /// Article sizes in posting order. More than one entry is what lets a part
    /// land short of its articles.
    segments: Vec<u32>,
    /// How many of those articles arrived.
    arrived_segments: usize,
    /// What the arrival actually left on disk.
    on_disk: Vec<u8>,
}

/// A part every article of which landed intact.
fn whole_split_join_part(filename: &str, bytes: &[u8]) -> SplitJoinPart {
    SplitJoinPart {
        filename: filename.to_string(),
        segments: vec![bytes.len() as u32],
        arrived_segments: 1,
        on_disk: bytes.to_vec(),
    }
}

/// A posting of plain split parts whose recovery set is computed over the file
/// the parts join into.
struct SplitJoinPosting {
    job_name: &'static str,
    joined: Vec<u8>,
    slice_size: u64,
    parts: Vec<SplitJoinPart>,
    /// Parts whose first 16 KiB the decode path captured. A part that begins
    /// where the joined file begins reproduces the joined description's 16 KiB
    /// hash exactly, which is how content binding finds it.
    prefix_captured: Vec<usize>,
    /// When set, the recovery set describes these files instead of the joined
    /// one — the ordinary shape, where the parts protect themselves.
    describes_parts: bool,
}

impl SplitJoinPosting {
    fn recovery_file_index(&self) -> u32 {
        self.parts.len() as u32
    }

    async fn install(&self, pipeline: &mut Pipeline, job_id: JobId) -> PathBuf {
        let recovery_bytes = vec![0xAAu8; 64];
        let mut files: Vec<FileSpec> = self
            .parts
            .iter()
            .enumerate()
            .map(|(index, part)| FileSpec {
                filename: part.filename.clone(),
                role: FileRole::from_filename(&part.filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: part
                    .segments
                    .iter()
                    .enumerate()
                    .map(|(ordinal, bytes)| {
                        segment_spec! {
                            number: ordinal as u32,
                            bytes: *bytes,
                            message_id: format!("split-join-{index}-{ordinal}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect();
        files.push(FileSpec {
            filename: SPLIT_JOIN_RECOVERY_FILENAME.to_string(),
            role: FileRole::from_filename(SPLIT_JOIN_RECOVERY_FILENAME),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: recovery_bytes.len() as u32,
                message_id: "split-join-recovery@example.com".to_string(),
            }],
        });

        let spec = JobSpec {
            name: self.job_name.to_string(),
            password: None,
            total_bytes: files
                .iter()
                .flat_map(|file| file.segments.iter())
                .map(|segment| u64::from(segment.bytes))
                .sum(),
            category: None,
            metadata: vec![],
            files,
        };
        let working_dir = insert_active_job(pipeline, job_id, spec).await;

        for (index, part) in self.parts.iter().enumerate() {
            let file_index = index as u32;
            tokio::fs::write(working_dir.join(&part.filename), &part.on_disk)
                .await
                .unwrap();
            let file_id = NzbFileId { job_id, file_index };
            {
                let state = pipeline.jobs.get_mut(&job_id).unwrap();
                let file = state.assembly.file_mut(file_id).unwrap();
                let mut offset = 0u64;
                for (ordinal, bytes) in part.segments.iter().enumerate().take(part.arrived_segments)
                {
                    file.record_placement(ordinal as u32, offset, *bytes);
                    file.commit_segment(ordinal as u32, *bytes).unwrap();
                    offset += u64::from(*bytes);
                }
            }
            if self.prefix_captured.contains(&index) {
                let window = part.on_disk.len().min(PAR2_HASH_16K_BYTES);
                pipeline
                    .file_prefix_16k
                    .insert(file_id, part.on_disk[..window].to_vec());
            }
            pipeline
                .refresh_archive_state_for_completed_file(job_id, file_id, true)
                .await;
        }

        write_and_complete_file(
            pipeline,
            job_id,
            self.recovery_file_index(),
            SPLIT_JOIN_RECOVERY_FILENAME,
            &recovery_bytes,
        )
        .await;

        let par2_set = if self.describes_parts {
            let described: Vec<(&str, &[u8])> = self
                .parts
                .iter()
                .map(|part| (part.filename.as_str(), part.on_disk.as_slice()))
                .collect();
            build_repairable_par2_set_for_files(
                &described,
                self.slice_size,
                SPLIT_JOIN_RECOVERY_BLOCKS as usize,
            )
        } else {
            build_repairable_par2_set(
                SPLIT_JOIN_JOINED_FILENAME,
                &self.joined,
                self.slice_size,
                SPLIT_JOIN_RECOVERY_BLOCKS as usize,
            )
        };
        install_test_par2_runtime(
            pipeline,
            job_id,
            par2_set,
            &[(
                self.recovery_file_index(),
                SPLIT_JOIN_RECOVERY_FILENAME,
                SPLIT_JOIN_RECOVERY_BLOCKS,
                true,
            )],
        );

        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
            state.status = JobStatus::Downloading;
            state.refresh_runtime_lanes_from_status();
        }

        working_dir
    }
}

/// Drive the completion gate the way a live pipeline would, resolving the
/// extraction tasks it spawns instead of racing them.
async fn settle_split_join_completion(pipeline: &mut Pipeline, job_id: JobId) {
    for _ in 0..12 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        while pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| !sets.is_empty())
        {
            let done = next_extraction_done(pipeline).await;
            pipeline.handle_extraction_done(done).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
    }
}

fn split_join_delivered_dir(pipeline: &Pipeline, job_name: &str) -> PathBuf {
    pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name))
}

fn delivered_entry_names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn split_join_failure_error(pipeline: &Pipeline, job_id: JobId) -> String {
    match job_status_for_assert(pipeline, job_id) {
        Some(JobStatus::Failed { error }) => error,
        _ => String::new(),
    }
}

/// A recovery set naming the joined file retires the split topology once it has
/// vouched for the join.
///
/// The parts carry the payload, the recovery data speaks for the file they
/// concatenate into, and the repair puts that file on disk from the parts
/// themselves. Joining them a second time afterwards would write the damaged
/// bytes back over the repaired ones, which is what shipped before: the
/// concatenation lands in staging, and staging wins the name in the final move.
#[tokio::test]
async fn a_recovery_set_naming_the_joined_file_retires_the_split_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30820);
    let job_name = "Ivory Meadow Split Join";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            // The middle part's slice is a hole on disk. Only reading the parts
            // as one file puts it back.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![64],
                arrived_segments: 1,
                on_disk: vec![0u8; 64],
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_some(),
        "precondition: the parts registered a split topology under the joined name"
    );

    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline.jobs.get(&job_id).is_none_or(|state| state
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_none()),
        "a verdict that vouched for the joined file must retire the topology that \
         would rebuild it; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {}",
        split_join_failure_error(&pipeline, job_id)
    );

    let drained = drain_job_events(&mut events, job_id);
    assert_eq!(
        drained
            .iter()
            .filter(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
            .count(),
        1,
        "exactly one repair is announced; events = {drained:?}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes, not a second concatenation"
    );
    let names = delivered_entry_names(&delivered);
    assert!(
        !names
            .iter()
            .any(|name| name.starts_with("Ivory.Meadow.mkv.")),
        "the parts a verified join consumed are not part of the release; delivered = {names:?}"
    );
}

/// A part short of its articles must not fail a job whose payload PAR2 has
/// already rebuilt.
///
/// With the joined file verified on disk, the split topology still wanted every
/// part whole, reported "no volumes are complete yet", and the completion gate
/// failed the job on that reason — after `RepairComplete` had already told the
/// UI the repair held.
#[tokio::test]
async fn a_split_part_short_of_articles_does_not_fail_a_rejoined_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30821);
    let job_name = "Ivory Meadow Short Part";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            // The second of this part's two articles never arrived, so the
            // part can never be called complete and its slice is short on disk.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![32, 32],
                arrived_segments: 1,
                on_disk: joined[64..96].to_vec(),
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline
            .classify_incomplete_after_par2(
                job_id,
                &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
                "post-verdict probe"
            )
            .is_none(),
        "a part a verified join consumed belongs in no incomplete bucket; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    let error = split_join_failure_error(&pipeline, job_id);
    assert!(
        !error.contains("no volumes are complete yet"),
        "a part short of articles is not a reason to fail a join PAR2 rebuilt; error = {error}"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {error}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes"
    );
}

/// A split fragment is not PAR2-protected before any join verdict exists.
#[tokio::test]
async fn a_part_sharing_the_joined_files_first_16_kib_is_unprotected_before_join_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30821);
    let slice_size = PAR2_HASH_16K_BYTES;
    let joined = split_join_payload(slice_size * 4);
    let posting = SplitJoinPosting {
        job_name: "Ivory Meadow Fragment Before Verdict",
        joined: joined.clone(),
        slice_size: slice_size as u64,
        parts: vec![
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.001".to_string(),
                segments: vec![slice_size as u32, slice_size as u32],
                arrived_segments: 1,
                on_disk: joined[..slice_size].to_vec(),
            },
            whole_split_join_part(
                "Ivory.Meadow.mkv.002",
                &joined[slice_size * 2..slice_size * 3],
            ),
            whole_split_join_part(
                "Ivory.Meadow.mkv.003",
                &joined[slice_size * 3..slice_size * 4],
            ),
        ],
        prefix_captured: vec![0],
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "a numeric split fragment must not content-bind to the joined description"
    );
    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "pre-verdict split fragment probe",
        )
        .expect("the incomplete fragment must still be classified");
    assert_eq!(
        report.unproven_protected, 0,
        "the fragment must not enter the protected bucket: {}",
        report.message
    );
    assert!(
        report.message.contains("unprotected"),
        "the report must identify the fragment as unprotected: {}",
        report.message
    );
}

/// A part sharing the joined file's first 16 KiB is not a hole in the payload.
///
/// The first part begins where the joined file begins, so its prefix matches
/// the joined description. Content binding refuses the part because its name
/// is a numeric split fragment of that description, and any declared yEnc size
/// that disagrees with the joined length corroborates the refusal. Even without
/// that refusal, the consumed-split exclusion after the join verdict shields
/// the part; this test keeps both layers honest.
#[tokio::test]
async fn a_part_sharing_the_joined_files_first_16_kib_is_not_reported_as_a_hole() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30822);
    let job_name = "Ivory Meadow Shared Prefix";
    let slice_size = PAR2_HASH_16K_BYTES;
    let joined = split_join_payload(slice_size * 4);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: slice_size as u64,
        parts: vec![
            // Two slices' worth, posted as two articles, of which only the
            // first arrived: the captured prefix still covers the description's
            // whole 16 KiB window.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.001".to_string(),
                segments: vec![slice_size as u32, slice_size as u32],
                arrived_segments: 1,
                on_disk: joined[..slice_size].to_vec(),
            },
            whole_split_join_part(
                "Ivory.Meadow.mkv.002",
                &joined[slice_size * 2..slice_size * 3],
            ),
            whole_split_join_part(
                "Ivory.Meadow.mkv.003",
                &joined[slice_size * 3..slice_size * 4],
            ),
        ],
        prefix_captured: vec![0],
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    let first_part = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert!(
        pipeline.resolve_par2_file_binding(first_part).is_none(),
        "the first part shares the joined file's first 16 KiB but is refused as its split \
         fragment — it must not bind to the joined description"
    );

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "a part a verified join consumed is not an outstanding protected file; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    let error = split_join_failure_error(&pipeline, job_id);
    assert!(
        !error.contains("BUG:") && !error.contains("nowhere on disk"),
        "a consumed part must not be reported as a missing protected file; error = {error}"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {error}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes"
    );
}

/// A join PAR2 already installed is never rebuilt over.
///
/// The guard belongs at the joiner even though a retired topology means the
/// gate does not normally dispatch it: an extraction spawned before the verdict
/// landed, or a restart that rebuilt the topology first, both reach the joiner
/// with the verified output already on disk.
#[tokio::test]
async fn a_joined_output_par2_installed_is_not_rebuilt_by_the_split_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30823);
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name: "Ivory Meadow Installed Join",
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![64],
                arrived_segments: 1,
                on_disk: vec![0u8; 64],
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    let working_dir = posting.install(&mut pipeline, job_id).await;

    // The state a successful repair leaves behind: the verified join on disk
    // beside the parts it was rebuilt from, and a settled verdict.
    let joined_path = working_dir.join(SPLIT_JOIN_JOINED_FILENAME);
    tokio::fs::write(&joined_path, &joined).await.unwrap();
    pipeline.par2_verified.insert(job_id);

    pipeline
        .extract_simple_archive(
            job_id,
            SPLIT_JOIN_JOINED_FILENAME,
            crate::pipeline::completion::finalize::SimpleArchiveKind::Split,
        )
        .await
        .unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;

    let staging = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.staging_dir.clone())
        .expect("the joiner reserves a staging directory either way");
    assert!(
        !staging.join(SPLIT_JOIN_JOINED_FILENAME).exists(),
        "the joiner must not write a second copy over a verified output; staging = {:?}",
        delivered_entry_names(&staging)
    );
    assert_eq!(
        tokio::fs::read(&joined_path).await.unwrap(),
        joined,
        "the verified join must survive untouched"
    );
    assert!(
        pipeline
            .extracted_members
            .get(&job_id)
            .is_some_and(|members| members.contains(SPLIT_JOIN_JOINED_FILENAME)),
        "the member is still reported extracted, so the gate moves on"
    );
}

/// The ordinary split posting, where the recovery set protects the parts.
///
/// Nothing here may change: no topology is retired, the parts are joined
/// exactly as they always were, and the join is the job's output.
#[tokio::test]
async fn a_recovery_set_naming_the_parts_still_joins_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30824);
    let job_name = "Ivory Meadow Protected Parts";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            whole_split_join_part("Ivory.Meadow.mkv.002", &joined[64..128]),
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: true,
    };
    posting.install(&mut pipeline, job_id).await;

    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_some(),
        "a verdict about the parts says nothing about the join, so the topology stands"
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {}",
        split_join_failure_error(&pipeline, job_id)
    );
    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the parts still join into the release's file"
    );
}

// ---------------------------------------------------------------------------
// The decode matrix's own budget
// ---------------------------------------------------------------------------

/// The shape that outgrows the transient decode-matrix budget.
///
/// The workspace a repair plan needs is set by the damage, not by streaming
/// buffer tuning: it grows with `missing²` plus `missing × total`, and the
/// budget it is measured against has a floor of its own well above weaver's
/// configured limit. Working the arithmetic backwards, nothing under
/// ~16,384 total slices can reach that floor at any damage level, and at the
/// format's 32,768-slice ceiling it takes more than ~11,994 missing slices —
/// upwards of a third of the set gone, with recovery blocks for every one of
/// them. No real posting is shaped like this; the point of pinning it is that
/// the refusal is a *budget* decision and says so.
const MATRIX_BUDGET_FILENAME: &str = "silver.horizon.bin";
const MATRIX_BUDGET_SLICE_SIZE: u64 = 16;
const MATRIX_BUDGET_TOTAL_SLICES: usize = 32_768;
const MATRIX_BUDGET_MISSING_SLICES: usize = 13_000;

/// Payload bytes with no repeating structure, so no damaged window can be
/// mistaken for an intact slice and the missing count is exactly what the
/// fixture punched out.
fn matrix_budget_payload() -> Vec<u8> {
    let mut data = vec![0u8; MATRIX_BUDGET_TOTAL_SLICES * MATRIX_BUDGET_SLICE_SIZE as usize];
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for chunk in data.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    data
}

/// The same payload with its first `MATRIX_BUDGET_MISSING_SLICES` slices
/// zeroed — aligned, so every surviving slice is still found where the set
/// describes it.
fn matrix_budget_damaged_payload(payload: &[u8]) -> Vec<u8> {
    let mut damaged = payload.to_vec();
    damaged[..MATRIX_BUDGET_MISSING_SLICES * MATRIX_BUDGET_SLICE_SIZE as usize].fill(0);
    damaged
}

/// Analysis only — never a repair. The budget decision is reached before any
/// planning, so this returns the verdict without spending a solve that, at this
/// shape, would be a 13,000-row field inversion.
fn analyze_with_memory_limit(
    working_dir: &std::path::Path,
    par2_set: &Par2FileSet,
    memory_limit: usize,
) -> par2_rs::Par2RepairOutcome {
    let mut options = par2_rs::Par2RepairerOptions::new(working_dir.to_path_buf(), Vec::new());
    options.file_set = Some(par2_set.clone());
    options.repair = false;
    options.memory_limit = Some(memory_limit);
    par2_rs::Par2Repairer::new(options)
        .verify_or_repair()
        .unwrap()
}

/// The budget is what refuses this set, and raising it is what accepts it.
///
/// The differential is the whole test: the same bytes, the same damage, the
/// same recovery — only the limit moves. That is what makes the refusal
/// actionable rather than a dead end, and it also settles what weaver's own
/// 64 MiB default has to do with it: nothing. The default is far below the
/// budget's floor, so the floor is what ruled, and only an explicitly larger
/// limit changes the answer.
#[test]
fn the_decode_matrix_budget_refuses_only_absurdly_damaged_sets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let payload = matrix_budget_payload();
    let par2_set = build_par2_set_with_uncomputed_recovery(
        MATRIX_BUDGET_FILENAME,
        &payload,
        MATRIX_BUDGET_SLICE_SIZE,
        MATRIX_BUDGET_MISSING_SLICES,
    );
    std::fs::write(
        temp_dir.path().join(MATRIX_BUDGET_FILENAME),
        matrix_budget_damaged_payload(&payload),
    )
    .unwrap();

    // What weaver actually configures.
    let refused = analyze_with_memory_limit(
        temp_dir.path(),
        &par2_set,
        crate::pipeline::completion::finalize::check::configured_par2_repair_memory_limit_bytes(),
    );
    assert_eq!(
        refused.verification.total_missing_blocks as usize, MATRIX_BUDGET_MISSING_SLICES,
        "the fixture must present exactly the damage it punched out"
    );
    match &refused.verification.repairable {
        par2_rs::verify::Repairability::ResourceLimited { reason } => assert!(
            reason.contains("matrix workspace budget"),
            "the refusal must be the workspace budget, not a format cap: {reason}"
        ),
        other => panic!("expected a resource-limited verdict, got {other:?}"),
    }

    // The same set with room for the workspace.
    let allowed = analyze_with_memory_limit(temp_dir.path(), &par2_set, 2 << 30);
    match &allowed.verification.repairable {
        par2_rs::verify::Repairability::Repairable { blocks_needed, .. } => assert_eq!(
            *blocks_needed as usize, MATRIX_BUDGET_MISSING_SLICES,
            "raising the limit must leave the damage untouched"
        ),
        other => panic!("expected a repairable verdict once the budget allows it, got {other:?}"),
    }
}

/// A job refused for a resource limit must say which knob exists.
///
/// The refusal used to reach the operator as a bare internal message with no
/// stated remedy, which is how a *tunable* limit ends up looking permanent.
#[tokio::test]
async fn a_resource_limited_par2_verdict_names_the_memory_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30470);
    let payload = matrix_budget_payload();
    let spec = standalone_job_spec(
        "Silver Horizon Beyond The Matrix Budget",
        &[(MATRIX_BUDGET_FILENAME.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_par2_set_with_uncomputed_recovery(
            MATRIX_BUDGET_FILENAME,
            &payload,
            MATRIX_BUDGET_SLICE_SIZE,
            MATRIX_BUDGET_MISSING_SLICES,
        ),
        &[],
    );
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        MATRIX_BUDGET_FILENAME,
        &matrix_budget_damaged_payload(&payload),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    match job_status_for_assert(&pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => {
            assert!(
                error.contains("WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES"),
                "a resource-limited failure must name the knob that raises the budget: {error}"
            );
            assert!(
                error.contains("matrix workspace budget"),
                "and must carry the verdict's own reason: {error}"
            );
        }
        other => panic!(
            "expected a resource-limited failure, got {other:?}; {}",
            debug_job_state(&pipeline, job_id)
        ),
    }
}

// ---------------------------------------------------------------------------
// What the quick pass is allowed to be blocked by
// ---------------------------------------------------------------------------

/// A clean payload beside a short *unprotected* file must not force a
/// whole-set read.
///
/// The quick pass answers for the recovery set, so the only thing that can stop
/// it is a file the set describes. An `.nfo` that lost an article is not one:
/// the pass could not have spoken for it either way, and letting it turn the
/// job away meant reading every byte of a payload already proven — the cost
/// that made a clean job look like a slow one.
#[tokio::test]
async fn a_short_unprotected_file_does_not_force_the_authoritative_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30471);
    let payload_filename = "silver.horizon.mkv";
    let nfo_filename = "silver.horizon.nfo";
    let payload: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
    let spec = JobSpec {
        name: "Silver Horizon Quick Path".to_string(),
        password: None,
        total_bytes: (payload.len() + 128) as u64,
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
                    bytes: payload.len() as u32,
                    message_id: "quick-path-payload@example.com".to_string(),
                }],
            },
            // Nothing in the recovery set describes this, and it is one article
            // short: the file the gate used to stall on.
            FileSpec {
                filename: nfo_filename.to_string(),
                role: FileRole::from_filename(nfo_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "quick-path-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "quick-path-nfo-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;
    tokio::fs::write(working_dir.join(nfo_filename), vec![7u8; 64])
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
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

    for _ in 0..12 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
    }

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        pipeline.par2_quick_verify_calls >= 1,
        "the quick pass is what should have answered; {}",
        debug_job_state(&pipeline, job_id)
    );
    // Both whole-set readers, since either one of them is the cost this
    // avoids: the placement pass and the repairer's own scan.
    assert_eq!(
        pipeline.par2_authoritative_verify_calls,
        0,
        "a file the recovery set never described must not buy a whole-set read; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_analyze_calls,
        0,
        "nor a whole-set repairer scan; {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A job with no parsed recovery set and two PAR2 files it could promote for
/// metadata: an index and a second index, neither yet tried.
async fn metadata_promotion_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
) -> (PathBuf, SegmentId, SegmentId) {
    let payload_filename = "silver-horizon.mkv";
    let first_index = "silver-horizon.par2";
    let second_index = "silver-horizon.vol00+01.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();

    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        total_bytes: payload.len() as u64 + 128,
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
                        message_id: "metadata-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "metadata-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: first_index.to_string(),
                role: FileRole::from_filename(first_index),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-index-a@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: second_index.to_string(),
                role: FileRole::from_filename(second_index),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-index-b@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    let first_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 1,
        },
        segment_number: 0,
    };
    let second_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };
    (working_dir, first_segment, second_segment)
}

fn drain_promoted_segments(pipeline: &mut Pipeline, job_id: JobId) -> Vec<SegmentId> {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state
        .download_queue
        .drain_all()
        .into_iter()
        .map(|work| work.segment_id)
        .collect()
}

/// A metadata candidate that arrived without yielding a set is finished.
///
/// The gate asks for metadata on every entry. A promoted index that completed
/// and parsed into nothing looks exactly like one that was never tried, so
/// without the promoted flag being consulted the same file is enqueued again on
/// every lap, forever.
#[tokio::test]
async fn a_promoted_metadata_candidate_is_never_promoted_twice() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30931);
    let (_, first_segment, _) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Once").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );

    // The index arrives and yields nothing a recovery set can be built from.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(first_segment.file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    assert!(
        pipeline.promote_par2_metadata(job_id),
        "a second candidate remains, so the job is still waiting"
    );
    let queued = drain_promoted_segments(&mut pipeline, job_id);
    assert!(
        !queued.contains(&first_segment),
        "the exhausted candidate must not be enqueued again; queued = {queued:?}"
    );
}

/// The second candidate takes its turn once the first is finished.
#[tokio::test]
async fn an_untried_metadata_candidate_follows_an_exhausted_one() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30932);
    let (_, first_segment, second_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Fallback").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );
    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![second_segment],
        "the untried candidate is the one that goes on the wire"
    );
}

/// Once every candidate has settled, promotion reports that it is finished.
///
/// The callers read `false` as "nothing can produce metadata" and own the
/// terminal failure from there; reporting `true` forever is what kept the job
/// alive with nothing left to try.
#[tokio::test]
async fn exhausted_metadata_candidates_stop_promising_metadata() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30933);
    let (_, first_segment, second_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Exhausted").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    drain_promoted_segments(&mut pipeline, job_id);
    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);
    assert!(pipeline.promote_par2_metadata(job_id));
    drain_promoted_segments(&mut pipeline, job_id);
    pipeline.mark_promoted_recovery_segment_unavailable(second_segment);

    assert!(
        !pipeline.promote_par2_metadata(job_id),
        "every candidate has settled, so the caller must be allowed to fail the job"
    );
    assert!(
        drain_promoted_segments(&mut pipeline, job_id).is_empty(),
        "and nothing may be enqueued on the way out"
    );
}

/// A candidate still on the wire keeps the job waiting without re-enqueuing it.
#[tokio::test]
async fn a_metadata_candidate_still_in_flight_enqueues_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30934);
    let (_, first_segment, _) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion In Flight").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );

    // Neither complete nor unavailable: the segment is still coming.
    assert!(
        pipeline.promote_par2_metadata(job_id),
        "the job is waiting for something real"
    );
    assert!(
        drain_promoted_segments(&mut pipeline, job_id).is_empty(),
        "but nothing is enqueued a second time"
    );
}

/// A repair's leftovers are shed even when a *later* set settles the job clean.
///
/// Purging by directory difference only ever ran in the repair tail, so a job
/// whose last set needed no repair never reached it: the earlier set's damaged
/// original stayed on disk and shipped with the payload. The aggregate settling
/// is the moment every set that was going to rewrite this directory has done
/// so, whichever kind of verdict happened to close it.
#[tokio::test]
async fn repair_leftovers_are_shed_when_a_clean_set_settles_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30935);
    let payload_filename = "onyx.prairie.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Leftovers After A Clean Set",
        payload_filename,
        &payload,
    )
    .await;
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );

    // An earlier set repaired here and left the damaged original behind.
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &payload).await.unwrap();

    // The job's last set settles clean — no repair tail runs for it at all.
    let set_id = pipeline.par2_set(job_id).unwrap().recovery_set_id;
    let _ = pipeline.mark_par2_set_verified(job_id, set_id).await;

    assert!(
        pipeline.par2_verified.contains(&job_id),
        "precondition: the aggregate settled on a clean verdict"
    );
    let delivered: std::collections::HashSet<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !leftover.exists(),
        "the damaged original must not be delivered; directory = {delivered:?}"
    );
    assert!(
        delivered.contains(payload_filename),
        "and the payload itself is untouched; directory = {delivered:?}"
    );
}

/// An index that can still arrive is not a residual to finalize around.
///
/// The shortcut that finalizes a job whose only incomplete files are archive
/// residuals also tolerated an incomplete PAR2 index, on the reasoning that a
/// job which already loaded a set has all the recovery data it is going to use.
/// That reasoning predates a posting carrying more than one set: a second index
/// still on the wire may describe files nothing has verified yet, and taking
/// the shortcut delivers them unchecked.
#[tokio::test]
async fn a_second_index_still_on_the_wire_is_not_an_ignorable_residual() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30936);
    let payload_filename = "silver-horizon.mkv";
    let first_index = "silver-horizon.par2";
    let second_index = "onyx-prairie.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let par2_file = |name: &str, tag: &str| FileSpec {
        filename: name.to_string(),
        role: FileRole::from_filename(name),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: format!("residual-{tag}@example.com"),
        }],
    };
    let spec = JobSpec {
        name: "Second Index Still Downloading".to_string(),
        password: None,
        total_bytes: payload.len() as u64 + 128,
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
                        message_id: "residual-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "residual-payload-1@example.com".to_string(),
                    },
                ],
            },
            par2_file(first_index, "index-a"),
            par2_file(second_index, "index-b"),
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    assert!(
        matches!(
            FileRole::from_filename(second_index),
            FileRole::Par2 { is_index: true, .. }
        ),
        "precondition: the second candidate really is an index"
    );

    // The payload and the first index are in; a set is loaded from the first.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for segment in [0u32, 1] {
            state
                .assembly
                .file_mut(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .commit_segment(segment, 64)
                .unwrap();
        }
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        // The second index is still coming: incomplete, and its article queued.
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("residual-index-b@example.com"),
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
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );

    assert!(
        !pipeline.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id),
        "a job whose second index is still downloading has not run out of \
         recovery sets to serve, so it must not take the finalization shortcut"
    );
}
