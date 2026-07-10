use super::*;

#[tokio::test]
async fn phase_end_removes_only_the_ended_phase_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(99);

    let repairing = pipeline.phase_begin(job_id, JobPhase::Repairing, Some(100));
    repairing.completed_bytes.store(25, Ordering::Relaxed);
    let extracting = pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    extracting.completed_bytes.store(50, Ordering::Relaxed);

    pipeline.sample_phase_progress();
    assert_eq!(
        pipeline.phase_progress_snapshots.get(&job_id).map(Vec::len),
        Some(2)
    );

    pipeline.phase_end(job_id, JobPhase::Extracting);

    let remaining = pipeline.phase_progress_snapshots.get(&job_id).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].phase, JobPhase::Repairing);
}

#[tokio::test]
async fn phase_end_extracting_if_idle_preserves_sibling_inflight_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100);

    pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("sibling-set".to_string());

    pipeline.phase_end_extracting_if_idle(job_id);

    assert!(
        pipeline
            .phase_progress
            .contains_key(&(job_id, JobPhase::Extracting)),
        "job-level extracting phase must stay live while any set is inflight"
    );

    pipeline.inflight_extractions.remove(&job_id);
    pipeline.phase_end_extracting_if_idle(job_id);

    assert!(
        !pipeline
            .phase_progress
            .contains_key(&(job_id, JobPhase::Extracting)),
        "extracting phase should end once all set and member work is idle"
    );
}

#[tokio::test]
async fn final_volume_refresh_heals_encrypted_multivolume_member_span_before_scheduling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40103);
    let set_name = "video";
    let member_name = "test_clip.mkv";
    let files = vec![
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
    let mut spec = rar_job_spec("RAR Facts Heal Encrypted Fixture Span", &files);
    spec.password = Some("testpass123".to_string());

    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate().take(4) {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, set_name)
        .expect("partial encrypted snapshot should exist after four volumes");
    let mut cached = serde_json::to_value(
        rmp_serde::from_slice::<weaver_unrar::CachedArchiveHeaders>(&cached_headers).unwrap(),
    )
    .unwrap();
    let members = cached["members"].as_array_mut().unwrap();
    let clip = members
        .iter_mut()
        .find(|member| member["name"] == member_name)
        .expect("cached snapshot should contain the encrypted clip member");
    let first_segment = clip["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("encrypted clip should retain its first segment in the snapshot");
    clip["segments"] = serde_json::json!([first_segment]);
    clip["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    pipeline
        .rar_sets
        .get_mut(&(job_id, set_name.to_string()))
        .expect("RAR set state should exist after partial arrival")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, set_name, &stale_headers)
        .unwrap();

    write_and_complete_rar_volume(&mut pipeline, job_id, 4, &files[4].0, &files[4].1).await;

    assert_eq!(
        member_span(&pipeline, job_id, set_name, member_name),
        Some((0, 4))
    );

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let selected = pipeline.volume_paths_for_rar_members(
        job_id,
        set_name,
        &[member_name.to_string()],
        &volume_paths,
        true,
        false,
    );
    assert_eq!(
        selected.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
}

#[tokio::test]
async fn verified_suspect_persist_serializes_latest_value_per_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40123);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Suspect Persist Queue", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.persist_verified_suspect_volumes(job_id, "show", &HashSet::from([1u32]));
    pipeline.persist_verified_suspect_volumes(job_id, "show", &HashSet::from([2u32, 3u32]));

    let key = (job_id, "show".to_string());
    let persist_state = pipeline
        .verified_suspect_persist_state
        .get(&key)
        .expect("verified suspect persistence state should exist");
    assert!(persist_state.in_flight_version.is_some());
    assert!(persist_state.queued);
    assert_eq!(persist_state.desired, HashSet::from([2u32, 3u32]));

    drain_verified_suspect_persists(&mut pipeline).await;

    assert!(
        pipeline
            .db
            .load_verified_suspect_volumes(job_id)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn submit_nzb_persists_zstd_and_creates_active_job() {
    let harness = TestHarness::new().await;
    let nzb_bytes = sample_nzb_bytes();

    let submitted = tokio::time::timeout(
        Duration::from_secs(2),
        submit_nzb_bytes(
            &harness.db,
            &harness.handle,
            &harness.config,
            &nzb_bytes,
            Some("Silver Horizon.Sample.nzb".to_string()),
            None,
            None,
            vec![("source".to_string(), "test".to_string())],
        ),
    )
    .await
    .unwrap()
    .unwrap();

    wait_until(Duration::from_secs(2), || {
        harness
            .db
            .load_active_jobs()
            .map(|jobs| jobs.contains_key(&submitted.job_id))
            .unwrap_or(false)
    })
    .await
    .unwrap();

    let info = harness.handle.get_job(submitted.job_id).unwrap();
    assert_eq!(info.status, JobStatus::Queued);
    assert!(
        info.metadata
            .contains(&("source".to_string(), "test".to_string()))
    );
    assert!(info.metadata.iter().any(|(key, value)| {
        key == "weaver.original_title" && value == "Silver Horizon.Sample"
    }));

    let (stored_path, stored_nzb) = harness
        .db
        .load_active_job_persisted_nzb(submitted.job_id)
        .unwrap()
        .and_then(|(path, nzb_zstd)| nzb_zstd.map(|nzb_zstd| (path, nzb_zstd)))
        .unwrap();
    assert_eq!(stored_path, PathBuf::from("Silver Horizon.Sample.nzb"));
    assert!(stored_nzb.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));

    let decoded = crate::ingest::decode_persisted_nzb_bytes(&stored_nzb).unwrap();
    assert_eq!(decoded, nzb_bytes);

    harness.shutdown().await;
}

#[tokio::test]
async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let job_name = "Silver Horizon Beyond the Vale";
    let payload_dir =
        "Silver Horizon.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
    let episode_name = "Silver Horizon.Beyond.Journeys.End.S01E01.mkv";

    let existing_dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    tokio::fs::create_dir_all(existing_dest.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(
        existing_dest.join(payload_dir).join(episode_name),
        b"existing",
    )
    .await
    .unwrap();

    let working_dir = intermediate_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(working_dir.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(payload_dir).join(episode_name), b"new")
        .await
        .unwrap();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );

    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    assert!(done.result.is_ok());
    pipeline.handle_move_to_complete_done(done);
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(pipeline.finished_jobs.iter().any(|job| job.job_id == job_id
        && job.output_dir.as_deref() == Some(expected_dest.to_str().unwrap())));
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"new"
    );
    assert_eq!(
        tokio::fs::read(existing_dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"existing"
    );
}

#[tokio::test]
async fn failed_final_move_marks_job_failed_instead_of_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10068);
    let job_name = "Silver Horizon Broken Final Move";
    let missing_working_dir = intermediate_dir.join("missing");

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, missing_working_dir),
    );

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(status, JobStatus::Failed { .. }));
    let JobStatus::Failed { error } = &status else {
        unreachable!();
    };
    assert!(error.contains("failed to read working directory"));
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
}

#[tokio::test]
async fn tiny_write_budget_evicts_out_of_order_segments_and_job_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20001);
    let filename = "episode.bin";
    let payload_size =
        (crate::runtime::buffers::BufferTier::Small.size_bytes() + 256 * 1024) as u32;
    let spec = segmented_job_spec(
        "Write Backlog Budget",
        filename,
        &[payload_size, payload_size, payload_size],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.write_backlog_budget_bytes = payload_size as usize;

    let total_size = payload_size as u64 * 3;
    for segment_number in [1u32, 2u32] {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            3,
            segment_number as u64 * payload_size as u64 + 1,
            total_size,
        );
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };

        pipeline.active_downloads += 1;
        tokio::time::timeout(
            Duration::from_secs(1),
            pipeline.handle_download_done(DownloadResult {
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
            }),
        )
        .await
        .expect("download completion should not block");
    }

    drain_decode_results(&mut pipeline, 2).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    wait_until(Duration::from_secs(2), || {
        pipeline
            .buffers
            .available(crate::runtime::buffers::BufferTier::Medium)
            == 1
    })
    .await
    .expect("decode scratch buffer should be returned after backlog relief");
    assert_eq!(
        pipeline
            .buffers
            .available(crate::runtime::buffers::BufferTier::Medium),
        1
    );
    assert!(
        pipeline
            .metrics
            .direct_write_evictions
            .load(Ordering::Relaxed)
            >= 1,
        "tiny write budget should degrade to direct writes"
    );
    assert!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed)
            <= payload_size as u64
    );

    let payload = vec![1u8; payload_size as usize];
    let raw = encode_article_part(filename, &payload, 1, 3, 1, total_size);
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Ok(DownloadPayload::Raw(raw)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;
    drain_decode_results(&mut pipeline, 1).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn in_order_segments_keep_write_cursor_until_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20007);
    let segment_count = 6u32;
    let payload_size = 128u32;
    let total_size = segment_count * payload_size;
    let filename = "cursor.bin";
    let spec = JobSpec {
        name: "Write Cursor".to_string(),
        password: None,
        total_bytes: total_size as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: (0..segment_count)
                .map(|number| {
                    segment_spec! {
                        number: number,
                        bytes: payload_size,
                        message_id: format!("cursor-{number}@example.com"),
                    }
                })
                .collect(),
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for segment_number in 0..segment_count {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            segment_count,
            segment_number as u64 * payload_size as u64 + 1,
            total_size as u64,
        );
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };

        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
            })
            .await;
    }

    drain_decode_results(&mut pipeline, segment_count as usize).await;

    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert!(!pipeline.write_buffers.contains_key(&NzbFileId {
        job_id,
        file_index: 0,
    }));
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        segment_count as u64
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn sparse_article_numbers_commit_cleanly_with_dense_ordinals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "sparse.bin";
    let payload_size = 128u32;
    let total_size = payload_size as u64 * 3;
    let spec = JobSpec {
        name: "Sparse Segment Ordinals".to_string(),
        password: None,
        total_bytes: total_size,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    ordinal: 0,
                    article_number: 1,
                    bytes: payload_size,
                    message_id: "sparse-0@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 1,
                    article_number: 4,
                    bytes: payload_size,
                    message_id: "sparse-1@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 2,
                    article_number: 6,
                    bytes: payload_size,
                    message_id: "sparse-2@example.com".to_string(),
                },
            ],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for segment_number in 0..3u32 {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            3,
            segment_number as u64 * payload_size as u64 + 1,
            total_size,
        );

        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                    segment_number,
                },
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
            })
            .await;
    }

    drain_decode_results(&mut pipeline, 3).await;

    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        3
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn hard_write_pressure_latches_until_soft_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1000;

    pipeline
        .metrics
        .write_buffered_bytes
        .store(1000, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Write.as_code()
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(701, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_stalls_total
            .load(Ordering::Relaxed),
        1
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(699, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::None.as_code()
    );
}

#[tokio::test]
async fn delayed_retry_drops_stale_exclusions_after_pool_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40041);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let segment_id = SegmentId {
        file_id,
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Stale Exclusion Rebuild".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "payload.bin".to_string(),
            role: FileRole::from_filename("payload.bin"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "stale-exclusion@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }

    let make_work = || DownloadWork {
        segment_id,
        message_id: MessageId::new("stale-exclusion@example.com"),
        groups: vec!["alt.binaries.test".to_string()],
        priority: 100,
        byte_estimate: 128,
        retry_count: 1,
        is_recovery: false,
        exclude_servers: vec![0],
    };

    // A rebuild bumped the generation after this retry was scheduled under gen 0:
    // its exclude index no longer refers to the server it was computed against,
    // so the guard must drop it (the retry re-tries every server cleanly).
    pipeline.pool_generation = 1;
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 0,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("stale retry should still requeue");
    assert_eq!(queued.segment_id, segment_id);
    assert!(
        queued.exclude_servers.is_empty(),
        "stale server exclusions must be dropped after a pool rebuild"
    );

    // No rebuild since scheduling: exclusions are still valid and preserved.
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 1,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("same-generation retry should requeue");
    assert_eq!(
        queued.exclude_servers,
        vec![0],
        "current-generation exclusions must be preserved"
    );
}

#[tokio::test]
async fn stale_extracted_member_checkpoint_is_discarded_during_completion_reconcile() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30085);
    let files = build_multifile_multivolume_rar_set();
    let _working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Discard Stale Extracted Member", &files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let member_name = "E01.mkv".to_string();
    let staging_dir = pipeline.extraction_staging_dir(job_id);
    let (out_path, partial_path) = Pipeline::member_output_paths(&staging_dir, &member_name);
    let chunk_dir = Pipeline::member_chunk_dir(&staging_dir, "show", &member_name);
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(&partial_path, b"episode-a-payload").unwrap();

    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert(member_name.clone());

    pipeline
        .db
        .replace_member_chunks(
            job_id,
            "show",
            &member_name,
            &[crate::ExtractionChunk {
                member_name: member_name.clone(),
                volume_index: 0,
                bytes_written: b"episode-a-payload".len() as u64,
                temp_path: partial_path.to_string_lossy().to_string(),
                start_offset: 0,
                end_offset: b"episode-a-payload".len() as u64,
                verified: true,
                appended: false,
            }],
        )
        .unwrap();

    let reconciled = pipeline
        .reconcile_extracted_outputs_for_completion(job_id)
        .await;

    assert!(reconciled);
    assert!(!out_path.exists());
    assert!(partial_path.exists());
    assert!(
        pipeline
            .db
            .get_extraction_chunks(job_id, "show")
            .unwrap()
            .into_iter()
            .all(|chunk| chunk.member_name != member_name)
    );
    assert!(!pipeline.extracted_members.contains_key(&job_id));
}

#[tokio::test]
async fn completion_reconciliation_clears_missing_extracted_outputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31234);
    let spec = JobSpec {
        name: "missing-output".to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline
        .extracted_members
        .insert(job_id, ["missing.mkv".to_string()].into_iter().collect());

    assert!(
        pipeline
            .reconcile_extracted_outputs_for_completion(job_id)
            .await
    );

    assert!(!pipeline.extracted_members.contains_key(&job_id));
    let recovered = pipeline.db.load_active_jobs().unwrap();
    assert!(
        recovered
            .get(&job_id)
            .is_some_and(|job| job.extracted_members.is_empty())
    );
}
