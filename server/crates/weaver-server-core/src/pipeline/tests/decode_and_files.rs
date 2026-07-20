use super::*;

#[tokio::test]
async fn pump_decode_queue_releases_bytes_for_inactive_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20027);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Inactive Decode Discard",
            &[("discard.bin".to_string(), 512u32)],
        ),
    )
    .await;

    let raw = Bytes::from_static(b"queued raw article");
    let raw_size = raw.len() as u64;
    pipeline.metrics.note_decode_work_queued(raw_size);
    pipeline.pending_decode.push_back(PendingDecodeWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        raw,
        source_server_idx: None,
        exclude_servers: Vec::new(),
    });
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Failed {
        error: "inactive".to_string(),
    };

    pipeline.pump_decode_queue();

    assert!(pipeline.pending_decode.is_empty());
    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.metrics.decode_active_bytes.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn pump_decode_queue_respects_decode_thread_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Decode Thread Limit",
            &[(("limited.bin").to_string(), 512u32)],
        ),
    )
    .await;

    let decode_limit = pipeline.tuner.params().decode_thread_count;
    let raw_size = 32usize;
    for segment_number in 0..(decode_limit + 3) {
        let raw = Bytes::from(vec![segment_number as u8; raw_size]);
        pipeline.metrics.note_decode_work_queued(raw.len() as u64);
        pipeline.pending_decode.push_back(PendingDecodeWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: segment_number as u32,
            },
            raw,
            source_server_idx: None,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.pump_decode_queue();

    assert_eq!(
        pipeline.active_decodes_by_job.values().sum::<usize>(),
        decode_limit
    );
    assert_eq!(pipeline.pending_decode.len(), 3);
    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 3);
    assert_eq!(
        pipeline
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        (3 * raw_size) as u64
    );
    assert_eq!(
        pipeline.metrics.decode_active_bytes.load(Ordering::Relaxed),
        (decode_limit * raw_size) as u64
    );
}

#[tokio::test]
async fn hard_decode_pressure_latches_until_soft_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.decode_backlog_budget_bytes = 1000;

    pipeline
        .metrics
        .decode_pending_bytes
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
        DownloadPressureReason::Decode.as_code()
    );

    pipeline
        .metrics
        .decode_pending_bytes
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
        .decode_pending_bytes
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
async fn decode_failure_drains_backlog_and_keeps_commands_responsive() {
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
    let job_id = JobId(20003);
    let files = vec![("broken.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Decode Failure", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline.active_downloads += 1;
    let raw = Bytes::from_static(b"not a yenc article");
    let raw_size = raw.len() as u64;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
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

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.metrics.decode_active_bytes.load(Ordering::Relaxed),
        raw_size
    );

    let done = tokio::time::timeout(Duration::from_secs(2), pipeline.decode_done_rx.recv())
        .await
        .expect("decode failure should arrive")
        .expect("decode channel should stay open");
    let DecodeDone::Failed {
        segment_id: failed_segment,
        raw_size: failed_raw_size,
        ..
    } = &done
    else {
        panic!("expected decode failure");
    };
    assert_eq!(*failed_segment, segment_id);
    assert_eq!(*failed_raw_size, raw_size);

    pipeline.handle_decode_done(done).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.metrics.decode_active_bytes.load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PauseAll { reply })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("pause reply should arrive")
        .unwrap();
    assert!(pipeline.global_paused);
    assert_eq!(
        pipeline.db.get_setting("global_paused").unwrap().as_deref(),
        Some("true")
    );

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ResumeAll { reply })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("resume reply should arrive")
        .unwrap();
    assert!(!pipeline.global_paused);
    assert_eq!(
        pipeline.db.get_setting("global_paused").unwrap().as_deref(),
        Some("false")
    );
}

#[tokio::test]
async fn decode_failure_retries_excluding_actual_source_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20014);
    let files = vec![("broken.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Decode Failure Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from_static(
                b"not a yenc article",
            ))),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    let done = tokio::time::timeout(Duration::from_secs(2), pipeline.decode_done_rx.recv())
        .await
        .expect("decode failure should arrive")
        .expect("decode channel should stay open");
    let DecodeDone::Failed {
        segment_id: failed_segment,
        source_server_idx,
        exclude_servers,
        ..
    } = &done
    else {
        panic!("expected decode failure");
    };
    assert_eq!(*failed_segment, segment_id);
    assert_eq!(*source_server_idx, Some(0));
    assert!(exclude_servers.is_empty());

    pipeline.handle_decode_done(done).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("decode failure should schedule a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
}

#[tokio::test]
async fn streamed_decode_failure_retries_excluding_actual_source_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let files = vec![("broken-streamed.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Streamed Decode Failure Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Decode {
                raw_size: 19,
                error: "missing =ybegin header".to_string(),
                crc_mismatch: false,
            }),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);
    assert!(pipeline.decode_done_rx.try_recv().is_err());

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("streamed decode failure should schedule a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
}

#[test]
fn decode_retry_exclude_servers_appends_actual_source_server_once() {
    assert_eq!(
        Pipeline::decode_retry_exclude_servers(&[], Some(1)),
        vec![1]
    );
    assert_eq!(
        Pipeline::decode_retry_exclude_servers(&[1], Some(1)),
        vec![1]
    );
    assert_eq!(
        Pipeline::decode_retry_exclude_servers(&[1], Some(0)),
        vec![1, 0]
    );
    assert_eq!(
        Pipeline::decode_retry_exclude_servers(&[1, 0], None),
        vec![1, 0]
    );
}

#[tokio::test]
async fn repeated_data_decode_failures_mark_failed_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
    let spec = segmented_job_spec("Repeated Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    for _ in 0..=MAX_SEGMENT_RETRIES {
        pipeline.handle_decode_failure(segment_id, "crc mismatch", &[], Some(0));
    }

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(64)
    );
}

#[tokio::test]
async fn recovery_decode_failures_do_not_mark_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let spec = standalone_with_par2_job_spec("Recovery Decode Failure", 128, 64);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };

    for _ in 0..=MAX_SEGMENT_RETRIES {
        pipeline.handle_decode_failure(segment_id, "crc mismatch", &[], None);
    }

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[test]
fn hdd_profile_allocates_more_write_backlog_than_ssd() {
    let profile = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 4,
            logical_cores: 4,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 8 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let mut hdd_profile = profile.clone();
    hdd_profile.disk.storage_class = StorageClass::Hdd;
    let buffers = BufferPool::new(BufferPoolConfig {
        small_count: 64,
        medium_count: 8,
        large_count: 2,
    });

    let ssd_budget = compute_write_backlog_budget_bytes(&profile, &buffers);
    let hdd_budget = compute_write_backlog_budget_bytes(&hdd_profile, &buffers);

    assert!(hdd_budget > ssd_budget);
}

#[test]
fn decode_backlog_budget_is_separate_bounded_raw_article_cache() {
    let profile = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 4,
            logical_cores: 4,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 8 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let buffers = BufferPool::new(BufferPoolConfig {
        small_count: 192,
        medium_count: 24,
        large_count: 6,
    });

    let write_budget = compute_write_backlog_budget_bytes(&profile, &buffers);
    let decode_budget = compute_decode_backlog_budget_bytes(&profile, &buffers, write_budget);

    assert!(decode_budget > write_budget);
    // Memory-scaled: available/12 dominates the buffer-pool term on an 8 GiB
    // machine, bounded by available/8.
    assert_eq!(decode_budget, 8 * 1024 * 1024 * 1024 / 12);
}

#[test]
fn decode_backlog_budget_respects_effective_memory_cap() {
    let profile = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 2,
            logical_cores: 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 512 * 1024 * 1024,
            available_bytes: 512 * 1024 * 1024,
            cgroup_limit: Some(512 * 1024 * 1024),
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 250.0,
            random_read_iops: 5_000.0,
            same_filesystem: true,
        },
    };
    let buffers = BufferPool::new(BufferPoolConfig {
        small_count: 32,
        medium_count: 4,
        large_count: 1,
    });

    let write_budget = compute_write_backlog_budget_bytes(&profile, &buffers);
    let decode_budget = compute_decode_backlog_budget_bytes(&profile, &buffers, write_budget);

    assert_eq!(decode_budget, 64 * 1024 * 1024);
}

#[tokio::test]
async fn fail_job_clears_write_backlog_accounting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20005);
    let spec = standalone_job_spec("Fail Clears Backlog", &[("stalled.bin".to_string(), 64u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 4096,
        data: DecodedChunk::from(vec![3u8; 4096]),
        part_crc: weaver_par2::checksum::crc32(&vec![3u8; 4096]),
        part_crc_verified: true,
        yenc_name: "stalled.bin".to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(8192, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    pipeline.fail_job(job_id, "forced failure".to_string());

    assert!(!pipeline.write_buffers.contains_key(&file_id));
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_segments
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn disk_write_failure_fails_job_before_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
    let spec = standalone_job_spec("Disk Write Failure", &[("blocked.bin".to_string(), 4u32)]);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working_dir.join("blocked.bin"))
        .await
        .unwrap();

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline
        .handle_decode_success(DecodeResult {
            segment_id: SegmentId {
                file_id,
                segment_number: 0,
            },
            raw_size: 4,
            unverified_provenance: None,
            file_offset: 0,
            decoded_size: 4,
            crc_valid: true,
            part_crc_verified: true,
            part_crc: weaver_par2::checksum::crc32(b"fail"),
            expected_file_crc: None,
            data: DecodedChunk::from(b"fail".to_vec()),
            yenc_name: "blocked.bin".to_string(),
        })
        .await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(
        &status,
        JobStatus::Failed { error } if error.contains("disk write failed")
    ));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(working_dir.join("blocked.bin").is_dir());
}

#[tokio::test]
async fn completed_standalone_file_crc32_match_persists_completion_without_md5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20017);
    let filename = "payload.bin";
    let payload = b"verified";
    let spec = standalone_job_spec(
        "Whole File CRC Match",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        Some(weaver_par2::checksum::crc32(payload)),
    )
    .await;

    assert!(pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.expected_file_crcs.contains_key(&file_id));
    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(!hashes.contains_key(&0));
}

#[test]
fn completed_file_checksum_combines_batched_decoded_crc_once() {
    let decoded = DecodedChunk::from(vec![
        b"verified-".to_vec().into_boxed_slice(),
        b"payload".to_vec().into_boxed_slice(),
    ]);
    assert!(matches!(decoded, DecodedChunk::Batches { .. }));

    let payload = b"verified-payload";
    let part_crc = checksum::crc32(payload);
    let mut state = CompletedFileChecksumState::new();

    state.update_decoded_chunk(&decoded, part_crc, true, false);

    let checksum = state.finalize();
    assert_eq!(checksum.crc32, part_crc);
    assert!(checksum.all_parts_crc_verified);
    assert!(checksum.md5.is_none());
}

#[tokio::test]
async fn deferred_decoded_data_hash_range_replays_verified_crc_metadata_without_reread() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20023);
    let filename = "payload.bin";
    let payload = b"abcdefgh";
    let spec = standalone_job_spec(
        "Verified Decoded Data Deferred Hash",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    pipeline
        .deferred_file_hash_ranges
        .entry(file_id)
        .or_default()
        .insert(
            0,
            DeferredFileHashRange {
                len: payload.len(),
                part_crc: weaver_par2::checksum::crc32(payload),
                part_crc_verified: true,
                source: DeferredFileHashRangeSource::DecodedData,
            },
        );

    let missing_path = temp_dir.path().join("missing-payload.bin");
    pipeline
        .drain_deferred_file_hash_ranges(file_id, &missing_path)
        .await;

    let state = pipeline
        .file_hash_states
        .get(&file_id)
        .expect("verified metadata should advance file hash state");
    assert_eq!(state.bytes_fed(), payload.len() as u64);
    assert_eq!(state.crc32(), weaver_par2::checksum::crc32(payload));
    assert!(state.all_parts_crc_verified());
    assert!(!state.tracks_md5());
    assert!(!pipeline.deferred_file_hash_ranges.contains_key(&file_id));
    assert!(!pipeline.file_hash_reread_required.contains(&file_id));
}

#[tokio::test]
async fn completed_standalone_verified_yenc_crc_skips_deferred_hash_replay() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20024);
    let filename = "payload.bin";
    let payload = b"abcdefgh";
    let spec = standalone_job_spec(
        "Verified CRC Deferred Hash",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    pipeline
        .deferred_file_hash_ranges
        .entry(file_id)
        .or_default()
        .insert(
            0,
            DeferredFileHashRange {
                len: 4,
                part_crc: weaver_par2::checksum::crc32(&payload[0..4]),
                part_crc_verified: true,
                source: DeferredFileHashRangeSource::CrcMetadata,
            },
        );
    pipeline
        .deferred_file_hash_ranges
        .entry(file_id)
        .or_default()
        .insert(
            4,
            DeferredFileHashRange {
                len: 4,
                part_crc: weaver_par2::checksum::crc32(&payload[4..8]),
                part_crc_verified: true,
                source: DeferredFileHashRangeSource::CrcMetadata,
            },
        );

    let checksum = pipeline
        .finalize_completed_file_hash(
            file_id,
            filename,
            temp_dir.path().join("missing-payload.bin"),
            payload.len() as u64,
            Some(weaver_par2::checksum::crc32(payload)),
        )
        .await
        .unwrap();

    assert_eq!(checksum.md5, None);
    assert_eq!(checksum.crc32, weaver_par2::checksum::crc32(payload));
    assert!(!pipeline.deferred_file_hash_ranges.contains_key(&file_id));
    assert!(!pipeline.deferred_file_hash_data.contains_key(&file_id));
    assert_eq!(pipeline.deferred_file_hash_data_bytes, 0);
    assert!(!pipeline.file_hash_states.contains_key(&file_id));
}

#[tokio::test]
async fn completed_standalone_deferred_crc_metadata_uses_actual_crc() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20025);
    let filename = "payload.bin";
    let payload = b"abcdefgh";
    let spec = standalone_job_spec(
        "Actual CRC Deferred Hash",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    pipeline
        .deferred_file_hash_ranges
        .entry(file_id)
        .or_default()
        .insert(
            0,
            DeferredFileHashRange {
                len: 4,
                part_crc: weaver_par2::checksum::crc32(&payload[0..4]),
                part_crc_verified: true,
                source: DeferredFileHashRangeSource::CrcMetadata,
            },
        );
    pipeline
        .deferred_file_hash_ranges
        .entry(file_id)
        .or_default()
        .insert(
            4,
            DeferredFileHashRange {
                len: 4,
                part_crc: weaver_par2::checksum::crc32(&payload[4..8]),
                part_crc_verified: false,
                source: DeferredFileHashRangeSource::CrcMetadata,
            },
        );

    let checksum = pipeline
        .finalize_completed_file_hash(
            file_id,
            filename,
            temp_dir.path().join("missing-payload.bin"),
            payload.len() as u64,
            Some(0xdead_beef),
        )
        .await
        .unwrap();

    assert_eq!(checksum.md5, None);
    assert_eq!(checksum.crc32, weaver_par2::checksum::crc32(payload));
    assert_ne!(checksum.crc32, 0xdead_beef);
    assert!(!pipeline.deferred_file_hash_ranges.contains_key(&file_id));
}

#[tokio::test]
async fn completed_file_uses_decoded_size_when_raw_article_bytes_are_larger() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20023);
    let filename = "payload.bin";
    let payload = b"decoded payload";
    let raw_article_bytes = payload.len() as u32 + 128;
    let spec = standalone_job_spec(
        "Decoded Size Completion",
        &[(filename.to_string(), raw_article_bytes)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let mut events = pipeline.event_tx.subscribe();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    pipeline
        .handle_decode_success(DecodeResult {
            segment_id: SegmentId {
                file_id,
                segment_number: 0,
            },
            raw_size: raw_article_bytes as u64,
            unverified_provenance: None,
            file_offset: 0,
            decoded_size: payload.len() as u32,
            crc_valid: true,
            part_crc_verified: true,
            part_crc: weaver_par2::checksum::crc32(payload),
            expected_file_crc: Some(weaver_par2::checksum::crc32(payload)),
            data: DecodedChunk::from(payload.to_vec()),
            yenc_name: filename.to_string(),
        })
        .await;

    let drained_events = drain_job_events(&mut events, job_id);
    assert!(drained_events.iter().any(|event| matches!(
        event,
        PipelineEvent::FileComplete { total_bytes, .. } if *total_bytes == payload.len() as u64
    )));
    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(!hashes.contains_key(&0));
}

#[tokio::test]
async fn verified_segment_keeps_crc_provenance_state_sparse() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20910);
    let filename = "verified.bin";
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Verified CRC Fast Path", filename, &[4, 4]),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment_with_part_crc_verified(
        &mut pipeline,
        file_id,
        0,
        0,
        b"abcd",
        filename,
        None,
        true,
    )
    .await;

    assert!(pipeline.unverified_segments.is_empty());
    assert!(pipeline.file_crc_recoveries.is_empty());
}

#[tokio::test]
async fn completing_file_removes_only_its_unverified_provenance_bucket() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let first_job_id = JobId(20911);
    let second_job_id = JobId(20912);
    for (job_id, name) in [
        (first_job_id, "first-unverified.bin"),
        (second_job_id, "second-unverified.bin"),
    ] {
        insert_active_job(
            &mut pipeline,
            job_id,
            segmented_job_spec("Per-file CRC Provenance", name, &[4, 4]),
        )
        .await;
        pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
        submit_decoded_segment_from_server(
            &mut pipeline,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            0,
            0,
            b"abcd",
            name,
            None,
            false,
            Some(0),
            Vec::new(),
        )
        .await;
    }

    let first_file_id = NzbFileId {
        job_id: first_job_id,
        file_index: 0,
    };
    let second_file_id = NzbFileId {
        job_id: second_job_id,
        file_index: 0,
    };
    assert_eq!(pipeline.unverified_segments.len(), 2);

    submit_decoded_segment_with_part_crc_verified(
        &mut pipeline,
        first_file_id,
        1,
        4,
        b"efgh",
        "first-unverified.bin",
        None,
        true,
    )
    .await;

    assert!(!pipeline.unverified_segments.contains_key(&first_file_id));
    assert!(pipeline.unverified_segments.contains_key(&second_file_id));
}

#[tokio::test]
async fn completed_file_crc32_mismatch_fails_before_persisting_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let filename = "payload.bin";
    let payload = b"damaged";
    let spec = standalone_job_spec(
        "Whole File CRC Mismatch",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        Some(0xDEADBEEF),
    )
    .await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(
        &status,
        JobStatus::Failed { error } if error.contains("whole-file CRC32 mismatch")
    ));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.expected_file_crcs.contains_key(&file_id));
    assert!(!pipeline.file_hash_states.contains_key(&file_id));
    assert!(!pipeline.file_hash_reread_required.contains(&file_id));
    assert!(
        pipeline
            .db
            .load_complete_file_hashes(job_id)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn whole_file_crc_mismatch_recovers_unverified_nonzero_segment_from_alternate_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20900);
    let filename = "payload.bin";
    let expected_payload = b"abcdefgh";
    let expected_crc = weaver_par2::checksum::crc32(expected_payload);
    let spec = two_segment_standalone_job_spec("CRC Recovery", filename, 4, 4);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        0,
        0,
        b"abcd",
        filename,
        Some(expected_crc),
        true,
        Some(0),
        Vec::new(),
    )
    .await;
    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        1,
        4,
        b"WXYZ",
        filename,
        Some(expected_crc),
        false,
        Some(0),
        Vec::new(),
    )
    .await;

    assert!(matches!(
        pipeline.jobs.get(&job_id).map(|state| &state.status),
        Some(JobStatus::Downloading)
    ));
    assert_eq!(
        pipeline
            .file_crc_recoveries
            .get(&file_id)
            .map(|recovery| recovery.pending_segments.len()),
        Some(1)
    );
    assert!(pipeline.job_has_pending_download_pipeline_work(job_id));
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .drain_all();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].segment_id.segment_number, 1);
    assert_eq!(queued[0].exclude_servers, vec![0]);
    assert!(
        !pipeline.db.load_active_jobs().unwrap()[&job_id]
            .file_progress
            .contains_key(&file_id.file_index),
        "CRC recovery must durably clear completed progress before re-download"
    );

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        1,
        4,
        b"efgh",
        filename,
        Some(expected_crc),
        true,
        Some(1),
        vec![0],
    )
    .await;

    assert_eq!(
        tokio::fs::read(working_dir.join(filename)).await.unwrap(),
        expected_payload
    );
    assert!(!pipeline.file_crc_recoveries.contains_key(&file_id));
    assert!(!pipeline.unverified_segments.contains_key(&file_id));
    assert_eq!(
        pipeline.jobs[&job_id].downloaded_bytes,
        expected_payload.len() as u64,
        "replacement bytes must not double-count logical progress"
    );
}

#[tokio::test]
async fn whole_file_crc_recovery_waits_for_entire_unverified_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(20901);
    let filename = "payload.bin";
    let expected_payload = b"abcdefgh";
    let expected_crc = weaver_par2::checksum::crc32(expected_payload);
    let spec = two_segment_standalone_job_spec("CRC Recovery Batch", filename, 4, 4);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (segment_number, offset, data) in [(0, 0, b"ABCD".as_slice()), (1, 4, b"WXYZ")] {
        submit_decoded_segment_from_server(
            &mut pipeline,
            file_id,
            segment_number,
            offset,
            data,
            filename,
            Some(expected_crc),
            false,
            Some(0),
            Vec::new(),
        )
        .await;
    }
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        0,
        0,
        b"abcd",
        filename,
        Some(expected_crc),
        true,
        Some(1),
        vec![0],
    )
    .await;
    assert_eq!(
        pipeline
            .file_crc_recoveries
            .get(&file_id)
            .map(|recovery| recovery.pending_segments.len()),
        Some(1)
    );
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::FileComplete { .. }))
    );

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        1,
        4,
        b"efgh",
        filename,
        Some(expected_crc),
        true,
        Some(1),
        vec![0],
    )
    .await;
    assert!(
        drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::FileComplete { .. }))
    );
}

#[tokio::test]
async fn matching_whole_file_crc_accepts_unverified_part_without_retry() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20902);
    let filename = "payload.bin";
    let payload = b"clean";
    let expected_crc = weaver_par2::checksum::crc32(payload);
    let spec = standalone_job_spec(
        "Unverified But Whole CRC Clean",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        Some(expected_crc),
        false,
        Some(0),
        Vec::new(),
    )
    .await;

    assert!(!pipeline.file_crc_recoveries.contains_key(&file_id));
    assert!(pipeline.jobs[&job_id].download_queue.is_empty());
    assert!(!pipeline.unverified_segments.contains_key(&file_id));
}

#[tokio::test]
async fn whole_file_crc_recovery_fails_when_unverified_segment_budget_is_exhausted() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20903);
    let filename = "payload.bin";
    let payload = b"wrong";
    let spec = standalone_job_spec(
        "CRC Recovery Exhausted",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let segment_id = SegmentId {
        file_id,
        segment_number: 0,
    };
    pipeline
        .decode_retries
        .insert(segment_id, MAX_SEGMENT_RETRIES);

    submit_decoded_segment_from_server(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        Some(0xDEAD_BEEF),
        false,
        Some(0),
        Vec::new(),
    )
    .await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(
        status,
        JobStatus::Failed { error } if error.contains("whole-file CRC32 mismatch")
    ));
    assert!(!pipeline.file_crc_recoveries.contains_key(&file_id));
    assert!(!pipeline.unverified_segments.contains_key(&file_id));
}

#[tokio::test]
async fn conflicting_file_crc32_across_segments_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20019);
    let filename = "payload.bin";
    let spec = two_segment_standalone_job_spec("Conflicting Whole CRC", filename, 4, 4);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        b"abcd",
        filename,
        Some(0x1111_1111),
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        4,
        b"efgh",
        filename,
        Some(0x2222_2222),
    )
    .await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(
        &status,
        JobStatus::Failed { error } if error.contains("conflicting yEnc whole-file CRC32")
    ));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.expected_file_crcs.contains_key(&file_id));
    assert!(!pipeline.file_hash_states.contains_key(&file_id));
    assert!(!pipeline.file_hash_reread_required.contains(&file_id));
    assert!(
        pipeline
            .db
            .load_complete_file_hashes(job_id)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn quiescent_tail_flush_completes_data_file_with_only_recovery_left() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20006);
    let spec = JobSpec {
        name: "Tail Flush".to_string(),
        password: None,
        total_bytes: 112,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "episode.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "data@example.com".to_string(),
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
                    message_id: "index@example.com".to_string(),
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
                    bytes: 32,
                    message_id: "repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered_payload = [9u8; 64];
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 64,
        data: DecodedChunk::from(buffered_payload.to_vec()),
        part_crc: weaver_par2::checksum::crc32(&buffered_payload),
        part_crc_verified: true,
        yenc_name: "episode.bin".to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(0, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state
        .assembly
        .file_mut(NzbFileId {
            job_id,
            file_index: 1,
        })
        .unwrap()
        .commit_segment(0, 16)
        .unwrap();
    state.download_queue = DownloadQueue::new();
    assert_eq!(state.recovery_queue.len(), 1);

    pipeline.flush_quiescent_write_backlog().await;
    settle_inflight_moves(&mut pipeline).await;

    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn quiescent_tail_flush_schedules_par2_analysis_when_recovery_is_parked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20007);
    let payload_filename = "episode.bin";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+02.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let spec = JobSpec {
        name: "Tail Flush PAR2 Promotion".to_string(),
        password: None,
        total_bytes: 128 + 16 + 32,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "data-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "data-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 16,
                    message_id: "index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 2,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 2, false),
        ],
    );

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 1,
        },
        decoded_size: 64,
        data: DecodedChunk::from(original_payload[64..].to_vec()),
        part_crc: weaver_par2::checksum::crc32(&original_payload[64..]),
        part_crc_verified: true,
        yenc_name: payload_filename.to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(64, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 16)
            .unwrap();
        state.download_queue = DownloadQueue::new();
        assert_eq!(state.recovery_queue.len(), 1);
    }

    assert!(pipeline.job_has_pending_download_pipeline_work(job_id));
    assert!(pipeline.pending_completion_checks.is_empty());

    pipeline.flush_quiescent_write_backlog().await;

    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
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

    let queued_job = pipeline.pending_completion_checks.pop_front().unwrap();
    pipeline.check_job_completion(queued_job).await;

    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(state.download_queue.has_recovery_work());
    assert!(state.recovery_queue.is_empty());
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn add_job_records_streamed_nzb_hash_in_active_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30036);
    let spec = standalone_job_spec("Streamed Hash", &[("episode.mkv".to_string(), 123)]);
    let nzb_path = PathBuf::from(format!("job-{}.nzb", job_id.0));
    let nzb_zstd = sample_nzb_zstd();
    let expected_hash = crate::ingest::hash_persisted_nzb_bytes(&nzb_zstd);

    pipeline
        .add_job(
            job_id,
            spec,
            nzb_path,
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(temp_dir.path().join("weaver.db"))
                .create_if_missing(false),
        )
        .await
        .unwrap();
    let stored_hash: Vec<u8> =
        sqlx::query_scalar("SELECT nzb_hash FROM active_jobs WHERE job_id = ?")
            .bind(job_id.0 as i64)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored_hash, expected_hash);
}

#[tokio::test]
async fn clean_par2_tar_requires_authoritative_verify_without_hash_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30119);
    let archive_filename = "payload.tar";
    let payload = b"plain tar payload";

    let tar_bytes = {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(payload.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, "payload.bin", &payload[..])
            .unwrap();
        builder.into_inner().unwrap()
    };

    let spec = standalone_job_spec(
        "Clean PAR2 Tar Requires Verify",
        &[(archive_filename.to_string(), tar_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), tar_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &tar_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Tar,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "payload.bin".to_string(),
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

    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
}

#[tokio::test]
async fn finalize_completed_file_hash_falls_back_to_disk_after_out_of_order_stream() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30117);
    let payload_filename = "payload.bin";
    let payload = b"abcdefgh";
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let file_path = temp_dir.path().join(payload_filename);
    tokio::fs::write(&file_path, payload).await.unwrap();

    pipeline.note_file_hash_chunk(
        file_id,
        4,
        &payload[4..8],
        weaver_par2::checksum::crc32(&payload[4..8]),
        true,
    );
    pipeline.note_file_hash_chunk(
        file_id,
        0,
        &payload[0..4],
        weaver_par2::checksum::crc32(&payload[0..4]),
        true,
    );

    let checksum = pipeline
        .finalize_completed_file_hash(
            file_id,
            payload_filename,
            file_path,
            payload.len() as u64,
            None,
        )
        .await
        .unwrap();
    assert_eq!(checksum.md5, Some(weaver_par2::checksum::md5(payload)));
    assert_eq!(checksum.crc32, weaver_par2::checksum::crc32(payload));
}

#[tokio::test]
async fn reprocess_job_rebuilds_failed_history_from_streamed_persisted_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30037);
    let mut row = history_row_with_output_dir(
        job_id,
        "Failed History Job",
        "failed",
        temp_dir.path().join("unused-output-dir"),
    );
    row.output_dir = None;
    row.error_message = Some("boom".to_string());
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.reprocess_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Extracting);
    assert_eq!(state.spec.files.len(), 1);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert_eq!(
        state.spec.metadata,
        vec![
            ("source".to_string(), "history".to_string()),
            (
                "weaver.original_title".to_string(),
                format!("job-{}", job_id.0),
            ),
        ]
    );
    assert!(state.download_queue.is_empty());
}

#[tokio::test]
async fn reprocess_job_rebuilds_complete_history_from_streamed_persisted_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30039);
    let mut row = history_row_with_output_dir(
        job_id,
        "Complete History Job",
        "complete",
        temp_dir.path().join("unused-output-dir"),
    );
    row.output_dir = None;
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.reprocess_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Extracting);
    assert_eq!(state.spec.files.len(), 1);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert_eq!(
        state.spec.metadata,
        vec![
            ("source".to_string(), "history".to_string()),
            (
                "weaver.original_title".to_string(),
                format!("job-{}", job_id.0),
            ),
        ]
    );
    assert!(state.download_queue.is_empty());
}
