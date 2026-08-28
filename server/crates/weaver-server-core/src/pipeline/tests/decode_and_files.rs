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

/// `weaver_pipeline_decode_task_duration_seconds` is absent until the decode
/// path has timed something, then reports exactly one observation per decode
/// task — the single clock read the task is allowed, taken once at its end
/// whichever way the task exits.
#[tokio::test]
async fn decode_tasks_record_one_wall_duration_each() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20033);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Decode Timing",
            &[("silver-horizon.bin".to_string(), 512u32)],
        ),
    )
    .await;

    assert!(
        pipeline
            .metrics
            .pipeline_histograms
            .snapshot()
            .decode_task_duration
            .is_none(),
        "the histogram must be absent, not an all-zero series, before the first task"
    );

    // The payload is not valid yEnc, so both tasks take the decode-failure
    // exit. That is deliberate: the measurement has to close on every path out
    // of the task, not only the happy one.
    for segment_number in 0..2u32 {
        let raw = Bytes::from_static(b"not a yenc article");
        pipeline.metrics.note_decode_work_queued(raw.len() as u64);
        pipeline.pending_decode.push_back(PendingDecodeWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            },
            raw,
            source_server_idx: None,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.pump_decode_queue();
    assert!(pipeline.pending_decode.is_empty());

    let metrics = Arc::clone(&pipeline.metrics);
    wait_until(Duration::from_secs(10), || {
        metrics
            .pipeline_histograms
            .snapshot()
            .decode_task_duration
            .is_some_and(|histogram| histogram.count == 2)
    })
    .await
    .expect("both decode tasks should record a duration");
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

#[tokio::test]
async fn queued_yenc_layout_mismatch_retries_before_decode_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20915);
    let filename = "queued-layout.bin";
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Queued Layout Mismatch", &[(filename.to_string(), 4)]),
    )
    .await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let mut events = pipeline.event_tx.subscribe();
    pipeline.metrics.note_decode_task_started(4);
    pipeline
        .handle_decode_done(DecodeDone::Success {
            result: DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size: 4,
                yenc_layout: YencLayoutAssertions {
                    file_size: 4,
                    part: Some(1),
                    total: Some(1),
                    begin: Some(u64::MAX),
                    end: Some(4),
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"data"),
                expected_file_crc: None,
                data: DecodedChunk::from(b"data".to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            },
            source: SegmentSource {
                source_server_idx: Some(1),
                exclude_servers: vec![2],
            },
        })
        .await;

    assert_eq!(pipeline.metrics.bytes_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.metrics.segments_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .map(|state| state.downloaded_bytes),
        Some(0)
    );
    assert!(!pipeline.write_buffers.contains_key(&segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("queued layout mismatch should schedule a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![2, 1]);
}

#[tokio::test]
async fn fused_yenc_layout_mismatch_retries_before_decode_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20916);
    let filename = "fused-layout.bin";
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Fused Layout Mismatch", &[(filename.to_string(), 4)]),
    )
    .await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let mut events = pipeline.event_tx.subscribe();
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Decoded(DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size: 8,
                yenc_layout: YencLayoutAssertions {
                    file_size: 5,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"data"),
                expected_file_crc: None,
                data: DecodedChunk::from(b"data".to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            })),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(2),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![3],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.metrics.bytes_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.metrics.segments_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .map(|state| state.downloaded_bytes),
        Some(0)
    );
    assert!(!pipeline.write_buffers.contains_key(&segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("fused layout mismatch should schedule a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![3, 2]);
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
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 4096,
        data: DecodedChunk::from(vec![3u8; 4096]),
        part_crc: par2_rs::checksum::crc32(&vec![3u8; 4096]),
        part_crc_verified: true,
        yenc_name: "stalled.bin".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments: Vec::new(),
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
async fn replayed_article_mid_download_still_completes_the_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20006);
    let filename = "replayed.bin";
    let payload = b"aaaabbbbcccc";
    let whole_file_crc = par2_rs::checksum::crc32(payload);
    let spec = segmented_job_spec("Replayed Article", filename, &[4, 4, 4]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let mut events = pipeline.event_tx.subscribe();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (segment_number, offset, chunk) in [
        (0u32, 0u64, &payload[0..4]),
        // The same article is delivered twice, landing behind the file's
        // write cursor. It must not stall the segments still to come.
        (0, 0, &payload[0..4]),
        (1, 4, &payload[4..8]),
        (2, 8, &payload[8..12]),
    ] {
        submit_decoded_segment(
            &mut pipeline,
            file_id,
            segment_number,
            offset,
            chunk,
            filename,
            Some(whole_file_crc),
        )
        .await;
    }

    // The whole-file CRC32 gate runs off the assembled bytes, so a completion
    // event here also says the replay did not corrupt what landed on disk.
    let drained_events = drain_job_events(&mut events, job_id);
    assert!(
        drained_events.iter().any(|event| matches!(
            event,
            PipelineEvent::FileComplete { total_bytes, .. }
                if *total_bytes == payload.len() as u64
        )),
        "a replayed article must not wedge the file's write reorder buffer: {drained_events:?}"
    );
    assert!(
        !drained_events
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "{drained_events:?}"
    );
    assert!(!pipeline.write_buffers.contains_key(&file_id));
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
}

/// A segmented spec whose file streams the *running* file hash on every commit.
///
/// A `Standalone` file that declares a whole-file CRC32 takes the deferred
/// CRC-metadata arm instead, which is keyed by offset and so never consults the
/// running offset at all. Only the streaming arm can observe a duplicate being
/// fed twice, and a RAR volume in a set with no PAR2 is the ordinary shape that
/// reaches it.
fn streaming_hash_job_spec(name: &str, filename: &str, segment_sizes: &[u32]) -> JobSpec {
    let mut spec = segmented_job_spec(name, filename, segment_sizes);
    spec.files[0].role = FileRole::RarVolume { volume_number: 0 };
    spec
}

#[tokio::test]
async fn replayed_article_mid_download_condemns_the_streamed_hash_but_completes_cleanly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // The fixture is a RAR volume so the STREAMING hash arm sees it — but a
    // routed article never feeds that arm at all, and with the gate at its
    // default-on the set would route. This test is about the conventional
    // streaming hash, so the gate is pinned off.
    pipeline
        .direct_store
        .set_gate(crate::pipeline::direct_store::DirectStoreGate::Disabled);
    let job_id = JobId(20007);
    let filename = "replayed-hash.part01.rar";
    let payload = b"aaaabbbbcccc";
    let whole_file_crc = par2_rs::checksum::crc32(payload);
    let spec = streaming_hash_job_spec("Replayed Article Hash", filename, &[4, 4, 4]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let mut events = pipeline.event_tx.subscribe();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &payload[0..4],
        filename,
        Some(whole_file_crc),
    )
    .await;
    assert_eq!(
        pipeline
            .file_hash_states
            .get(&file_id)
            .map(|state| state.bytes_fed()),
        Some(4)
    );

    // The same article again, landing behind the file's write cursor. The
    // arrival rewrote the range on disk, and nothing at the commit seam can
    // prove it wrote the same bytes the stream already digested — CRC
    // equality is not byte identity — so the streamed state is condemned to
    // the completion-time re-read, which digests the disk as the rewrite
    // left it. The duplicate's bytes are still not fed to the running hash:
    // the poison replaces the stream, it does not double-feed it.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &payload[0..4],
        filename,
        Some(whole_file_crc),
    )
    .await;
    assert!(
        pipeline.file_hash_reread_required.contains(&file_id),
        "a duplicate rewrite must condemn the streamed hash state to a re-read"
    );
    assert_eq!(
        pipeline
            .file_hash_states
            .get(&file_id)
            .map(|state| state.bytes_fed()),
        None,
        "the condemned streaming state must be dropped, not fed further"
    );

    for (segment_number, offset, chunk) in [(1u32, 4u64, &payload[4..8]), (2, 8, &payload[8..12])] {
        submit_decoded_segment(
            &mut pipeline,
            file_id,
            segment_number,
            offset,
            chunk,
            filename,
            Some(whole_file_crc),
        )
        .await;
    }

    // `expected_file_crc` is supplied, so a wrong CRC32 fails the job rather
    // than completing it: FileComplete here is the whole-file gate passing on
    // the completion-time re-read of the bytes the rewrite left on disk.
    let drained_events = drain_job_events(&mut events, job_id);
    assert!(
        drained_events.iter().any(|event| matches!(
            event,
            PipelineEvent::FileComplete { total_bytes, .. }
                if *total_bytes == payload.len() as u64
        )),
        "{drained_events:?}"
    );
    assert!(
        !drained_events
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobFailed { .. })),
        "{drained_events:?}"
    );
}

#[tokio::test]
async fn late_metadata_conflicting_duplicate_cannot_quick_verify_against_stale_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20017);
    let filename = "late-metadata-dup.bin";
    // The original first article and its same-length rewrite.
    let original: &[u8] = b"aaaabbbbcccc";
    let disk_final: &[u8] = b"zzzzbbbbcccc";
    let whole_file_crc = par2_rs::checksum::crc32(disk_final);
    let spec = segmented_job_spec("Late Metadata Conflicting Duplicate", filename, &[4, 4, 4]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let mut events = pipeline.event_tx.subscribe();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // No PAR2 metadata exists yet: nothing records block verdicts, so if the
    // streamed digest survived the rewrite below, no verdict could veto it.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &original[0..4],
        filename,
        Some(whole_file_crc),
    )
    .await;
    // The duplicate rewrites the range with DIFFERENT same-length bytes.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &disk_final[0..4],
        filename,
        Some(whole_file_crc),
    )
    .await;
    assert!(pipeline.file_hash_reread_required.contains(&file_id));
    for (segment_number, offset, chunk) in
        [(1u32, 4u64, &disk_final[4..8]), (2, 8, &disk_final[8..12])]
    {
        submit_decoded_segment(
            &mut pipeline,
            file_id,
            segment_number,
            offset,
            chunk,
            filename,
            Some(whole_file_crc),
        )
        .await;
    }
    let drained = drain_job_events(&mut events, job_id);
    assert!(
        drained.iter().any(|event| matches!(
            event,
            PipelineEvent::FileComplete { total_bytes, .. }
                if *total_bytes == disk_final.len() as u64
        )),
        "{drained:?}"
    );

    // Whatever completion persisted must describe the disk as the rewrite
    // left it, never the pre-rewrite stream.
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_ne!(
        trusted.get(&0).copied(),
        Some(par2_rs::checksum::md5(original)),
        "the pre-rewrite digest must not be persisted as trusted"
    );

    // PAR2 metadata arrives only now, describing the ORIGINAL bytes.
    let mut par2_set = placement_par2_file_set(&[(filename.to_string(), original.to_vec())]);
    let par2_file_id = par2_set.recovery_file_ids[0];
    let slice_checksum = {
        let mut state = par2_rs::SliceChecksumState::new();
        state.update(original);
        let (crc32, md5) = state.finalize(Some(original.len() as u64));
        par2_rs::SliceChecksum { crc32, md5 }
    };
    par2_set
        .slice_checksums
        .insert(par2_file_id, vec![slice_checksum]);
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;

    // Quick verification refuses — the honest digest mismatches the
    // description, the grid never observed these articles, and no stale
    // trusted hash exists to lie with — so the authoritative pass runs.
    assert!(drain_job_verification_started(&mut events, job_id) >= 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn mismatched_out_of_order_offset_is_rejected_before_hash_state_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // Pinned off for the same reason as the replayed-article test above: the
    // RAR-volume fixture exists to reach the streaming hash arm, which a
    // routed article bypasses by design.
    pipeline
        .direct_store
        .set_gate(crate::pipeline::direct_store::DirectStoreGate::Disabled);
    let job_id = JobId(20008);
    let filename = "out-of-order-hash.part01.rar";
    let payload = b"aaaabbbbcccc";
    let whole_file_crc = par2_rs::checksum::crc32(payload);
    let spec = streaming_hash_job_spec("Out Of Order Hash", filename, &[4, 4, 4]);
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
        &payload[0..4],
        filename,
        Some(whole_file_crc),
    )
    .await;
    assert!(!pipeline.file_hash_reread_required.contains(&file_id));

    // A different segment claiming the first segment's range is rejected at
    // the NZB/yEnc boundary, before it can poison the streaming hash state.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        0,
        &payload[4..8],
        filename,
        Some(whole_file_crc),
    )
    .await;
    assert!(!pipeline.file_hash_reread_required.contains(&file_id));
    assert!(pipeline.file_hash_states.contains_key(&file_id));
    assert_eq!(
        pipeline.decode_retries.get(&SegmentId {
            file_id,
            segment_number: 1,
        }),
        Some(&1)
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
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: SegmentId {
                    file_id,
                    segment_number: 0,
                },
                raw_size: 4,
                yenc_layout: YencLayoutAssertions {
                    file_size: 4,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"fail"),
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(b"fail".to_vec()),
                yenc_name: "blocked.bin".to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
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
        Some(par2_rs::checksum::crc32(payload)),
    )
    .await;

    assert!(pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.expected_file_crcs.contains_key(&file_id));
    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(!hashes.contains_key(&0));
}

#[tokio::test]
async fn completed_par2_covered_file_skips_streamed_md5_without_posted_file_crc() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let filename = "payload.bin";
    let payload = b"verified";
    let spec = standalone_job_spec(
        "Par2 Covered No Posted CRC",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(filename, payload, 4, 0),
        &[],
    );
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // No aggregate `=yend crc32` accompanies the post: the recovery set's
    // IFSC checksums alone license the skip, so nothing streams a whole-file
    // MD5 and nothing reads the file back for one at completion.
    submit_decoded_segment(&mut pipeline, file_id, 0, 0, payload, filename, None).await;

    let checksum = pipeline
        .par2_runtime(job_id)
        .and_then(|runtime| runtime.completed_checksums.get(&file_id))
        .copied()
        .expect("completed checksum recorded for par2-covered file");
    assert!(checksum.md5.is_none());
    assert_eq!(checksum.crc32, par2_rs::checksum::crc32(payload));
    assert!(checksum.all_parts_crc_verified);
    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(!hashes.contains_key(&0));
}

#[tokio::test]
async fn par2_md5_substitution_resolves_once_at_lifecycle_not_per_chunk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(200180);
    let covered_name = "covered.bin";
    let unbound_name = "unbound.bin";
    let covered = b"covered";
    let unbound = b"unbound";
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Lifecycle MD5 Binding",
            &[
                (covered_name.to_string(), covered.len() as u32),
                (unbound_name.to_string(), unbound.len() as u32),
            ],
        ),
    )
    .await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(covered_name, covered, 4, 0),
        &[],
    );
    let covered_file = NzbFileId {
        job_id,
        file_index: 0,
    };
    let unbound_file = NzbFileId {
        job_id,
        file_index: 1,
    };

    pipeline
        .par2_binding_resolver_calls
        .store(0, std::sync::atomic::Ordering::Relaxed);
    pipeline.refresh_par2_md5_substitution_bindings(job_id);
    assert_eq!(
        pipeline
            .par2_binding_resolver_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        2,
        "the lifecycle pass resolves each assembly file once"
    );
    assert!(pipeline.par2_md5_substitution_is_cached(covered_file));
    assert!(!pipeline.par2_md5_substitution_is_cached(unbound_file));

    pipeline
        .par2_binding_resolver_calls
        .store(0, std::sync::atomic::Ordering::Relaxed);
    pipeline.note_file_hash_chunk(
        covered_file,
        0,
        &covered[..3],
        par2_rs::checksum::crc32(&covered[..3]),
        true,
    );
    pipeline.note_file_hash_chunk(
        covered_file,
        3,
        &covered[3..],
        par2_rs::checksum::crc32(&covered[3..]),
        true,
    );
    pipeline.note_file_hash_chunk(
        unbound_file,
        0,
        unbound,
        par2_rs::checksum::crc32(unbound),
        true,
    );

    assert_eq!(
        pipeline
            .par2_binding_resolver_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "article chunks only read the positive cache"
    );
    assert!(
        !pipeline.file_hash_states[&covered_file].tracks_md5()
            && pipeline.file_hash_states[&unbound_file].tracks_md5(),
        "an unbound file keeps streaming MD5"
    );
}

#[tokio::test]
async fn an_unbound_file_cannot_borrow_another_sets_grid_to_skip_md5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(200181);
    let covered_name = "covered.bin";
    let unbound_name = "unbound.bin";
    let covered = b"covered";
    let unbound = b"unbound";
    let spec = standalone_job_spec(
        "Bound Set Cannot Lend MD5 Evidence",
        &[
            (covered_name.to_string(), covered.len() as u32),
            (unbound_name.to_string(), unbound.len() as u32),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(covered_name, covered, 4, 0),
        &[],
    );
    let unbound_file = NzbFileId {
        job_id,
        file_index: 1,
    };

    submit_decoded_segment(
        &mut pipeline,
        unbound_file,
        0,
        0,
        unbound,
        unbound_name,
        None,
    )
    .await;

    assert!(
        pipeline.resolve_par2_file_binding(unbound_file).is_none(),
        "the second file is not described by the first set"
    );
    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert!(
        hashes.contains_key(&1),
        "an unbound file must retain its own streamed MD5 rather than borrowing set A's proof"
    );
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
                part_crc: par2_rs::checksum::crc32(payload),
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
    assert_eq!(state.crc32(), par2_rs::checksum::crc32(payload));
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
                part_crc: par2_rs::checksum::crc32(&payload[0..4]),
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
                part_crc: par2_rs::checksum::crc32(&payload[4..8]),
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
            Some(par2_rs::checksum::crc32(payload)),
        )
        .await
        .unwrap();

    assert_eq!(checksum.md5, None);
    assert_eq!(checksum.crc32, par2_rs::checksum::crc32(payload));
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
                part_crc: par2_rs::checksum::crc32(&payload[0..4]),
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
                part_crc: par2_rs::checksum::crc32(&payload[4..8]),
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
    assert_eq!(checksum.crc32, par2_rs::checksum::crc32(payload));
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
    let raw_article_bytes = payload.len() as u64 + 128;
    let spec = standalone_job_spec(
        "Decoded Size Completion",
        &[(filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let mut events = pipeline.event_tx.subscribe();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: SegmentId {
                    file_id,
                    segment_number: 0,
                },
                raw_size: raw_article_bytes,
                yenc_layout: YencLayoutAssertions {
                    file_size: payload.len() as u64,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(payload),
                expected_file_crc: Some(par2_rs::checksum::crc32(payload)),
                segments: Vec::new(),
                data: DecodedChunk::from(payload.to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
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
    let expected_crc = par2_rs::checksum::crc32(expected_payload);
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
    let expected_crc = par2_rs::checksum::crc32(expected_payload);
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
    let expected_crc = par2_rs::checksum::crc32(payload);
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
    pipeline.par2_bypassed.insert(job_id);

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered_payload = [9u8; 64];
    let buffered = BufferedDecodedSegment {
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 64,
        data: DecodedChunk::from(buffered_payload.to_vec()),
        part_crc: par2_rs::checksum::crc32(&buffered_payload),
        part_crc_verified: true,
        yenc_name: "episode.bin".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments: Vec::new(),
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
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 1,
        },
        decoded_size: 64,
        data: DecodedChunk::from(original_payload[64..].to_vec()),
        part_crc: par2_rs::checksum::crc32(&original_payload[64..]),
        part_crc_verified: true,
        yenc_name: payload_filename.to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments: Vec::new(),
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

    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline.flush_quiescent_write_backlog().await;
    assert!(pipeline.write_buffered_bytes > 0);
    assert!(pipeline.pending_completion_checks.is_empty());
    pipeline.active_downloads_by_job.remove(&job_id);

    let competing_job_id = JobId(20008);
    insert_active_job(
        &mut pipeline,
        competing_job_id,
        standalone_job_spec("Active Competitor", &[("competitor.bin".to_string(), 64)]),
    )
    .await;
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(competing_job_id, 1);

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
        par2_rs::checksum::crc32(&payload[4..8]),
        true,
    );
    pipeline.note_file_hash_chunk(
        file_id,
        0,
        &payload[0..4],
        par2_rs::checksum::crc32(&payload[0..4]),
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
    assert_eq!(checksum.md5, Some(par2_rs::checksum::md5(payload)));
    assert_eq!(checksum.crc32, par2_rs::checksum::crc32(payload));
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

// ---- uuencode sequential assembly ----

/// Submit one uuencode part.
///
/// Every argument is in DECODED units except the job spec's declared sizes,
/// which stay encoded exactly as an NZB would carry them — that mismatch is the
/// whole point of these tests.
async fn submit_uu_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
) {
    submit_uu_segment_named(
        pipeline,
        file_id,
        segment_number,
        data,
        damaged,
        ended,
        "silver-horizon.bin",
    )
    .await;
}

/// Submit one uuencode part, stating what its `begin` header called the file.
///
/// A real post states the name exactly once, on the part that opens the body,
/// and continuation parts carry none at all — which is what `""` means here.
#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_uu_segment_named(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
    begin_name: &str,
) {
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts { damaged, ended }),
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: false,
                part_crc: 0,
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: begin_name.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

/// A job whose declared segment sizes are uuencode-encoded (~1.38x the decoded
/// bytes), which is what an NZB really carries for a uuencode post.
fn uu_job_spec(decoded_sizes: &[usize]) -> JobSpec {
    let declared: Vec<u32> = decoded_sizes
        .iter()
        .map(|len| (*len as f64 * 1.38).ceil() as u32)
        .collect();
    let mut spec = standalone_job_spec(
        "Silver Horizon UU",
        &[("silver-horizon.bin".to_string(), declared[0])],
    );
    let file = &mut spec.files[0];
    file.segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        file.segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("uu-{index}@example.com"),
        });
    }
    spec
}

#[tokio::test]
async fn uu_segments_assemble_sequentially_at_decoded_offsets() {
    // Placement must come from cumulative DECODED lengths. The declared sizes
    // in the spec are ~1.38x larger; if any of them leaked into the offset
    // computation the file would be scattered and this comparison would fail.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20061);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 900], vec![b'b'; 700], vec![b'c'; 350]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (index, part) in parts.iter().enumerate() {
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
    }

    let expected: Vec<u8> = parts.concat();
    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, expected);

    // Direct store is excluded: the bytes materialised as an ordinary file on
    // the conventional assembly path even though this pipeline has direct
    // routing enabled.
    assert!(working_dir.join("silver-horizon.bin").is_file());
    // And the dual-CRC grid was never fed, so it can claim nothing.
    assert!(pipeline.block_crc_verdicts(file_id).is_none());
}

#[tokio::test]
async fn uu_segments_park_until_their_prefix_arrives() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20062);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 600], vec![b'b'; 450], vec![b'c'; 300]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Reverse order: nothing can be placed until part 0 lands.
    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
        Some(2),
        "both later parts must be waiting on their prefix"
    );

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, parts.concat());
}

#[tokio::test]
async fn uu_duplicate_segment_is_replaced_at_its_original_offset() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20063);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 500], vec![b'b'; 400]];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    // The same article again: it must land on its own bytes, not shift the
    // cursor or append.
    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, true).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(
        written,
        parts.concat(),
        "a duplicate must not lengthen the file"
    );
}

#[tokio::test]
async fn uu_single_segment_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20064);
    let payload = vec![b'z'; 777];
    let working_dir = insert_active_job(&mut pipeline, job_id, uu_job_spec(&[payload.len()])).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &payload, false, true).await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, payload);
}

#[tokio::test]
async fn uu_missing_middle_shifts_survivors_and_marks_damage() {
    // A part that will never arrive cannot wedge the file forever. The cursor
    // closes the gap, which shifts every later part down by the hole's width —
    // the file's tail is MISALIGNED, not merely short. The file is flagged
    // damaged so nothing downstream reads it as clean; PAR2 is the authority on
    // whether it can be recovered, and without PAR2 it is simply wrong.
    //
    // Ten parts with one failure keeps job health above its critical floor, so
    // this exercises the shift itself. The companion test below pins what
    // happens when the failure rate crosses that floor instead.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20065);
    let parts: Vec<Vec<u8>> = (0..10u8)
        .map(|i| vec![b'a' + i; 100 + i as usize])
        .collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    const HOLE: u32 = 3;
    for (index, part) in parts.iter().enumerate() {
        if index as u32 == HOLE {
            continue;
        }
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
    }

    // Everything from the hole onward is still parked: the cursor cannot know
    // where part 4 begins until part 3's decoded length is known.
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.next_index),
        Some(HOLE),
        "the cursor waits at the hole"
    );

    // Part 3 exhausts its retries. `book_failed_segment` also runs the job
    // health policy, which in a synthetic pipeline with no recorded successful
    // fetches fails the job outright; that policy is pinned separately by
    // `uu_file_losing_too_many_segments_fails_the_job`. Here the gap-closing
    // step is exercised on its own.
    pipeline.skip_failed_uu_segment(SegmentId {
        file_id,
        segment_number: HOLE,
    });

    assert!(
        pipeline.uu_files.get(&file_id).is_some_and(|uu| uu.damaged),
        "closing a gap must mark the file damaged"
    );
    // Terminal, not wedged: the job survived this failure and the cursor moved.
    assert!(pipeline.jobs.contains_key(&job_id));

    // Re-delivering a survivor now lands it at the shifted offset.
    submit_uu_segment(
        &mut pipeline,
        file_id,
        HOLE + 1,
        &parts[(HOLE + 1) as usize],
        false,
        false,
    )
    .await;

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    let mut expected: Vec<u8> = Vec::new();
    for part in parts.iter().take(HOLE as usize) {
        expected.extend_from_slice(part);
    }
    expected.extend_from_slice(&parts[(HOLE + 1) as usize]);
    assert!(
        written.starts_with(&expected),
        "survivors must commit at offsets shifted down over the hole"
    );
}

#[tokio::test]
async fn uu_file_losing_too_many_segments_fails_the_job() {
    // The observed policy when a uuencode file loses enough parts to cross the
    // job health floor: the job fails outright rather than assembling a badly
    // misaligned file. Pinned as-observed — this is the existing no-PAR2
    // damaged-file behaviour, not a uuencode-specific rule.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20067);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 300], vec![b'b'; 200], vec![b'c'; 250]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    pipeline.book_failed_segment(SegmentId {
        file_id,
        segment_number: 1,
    });

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    // Teardown released the parked bytes with the write buffers.
    assert!(!pipeline.uu_files.contains_key(&file_id));
}

#[tokio::test]
async fn uu_file_missing_its_end_marker_is_damaged() {
    // Every listed ordinal arrived, but the post itself was truncated: no
    // `end` line ever appeared. uuencode ships no checksum, so this is the only
    // signal that the file is short.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20066);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 400], vec![b'b'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    let state_before = pipeline
        .uu_files
        .get(&file_id)
        .map(|uu| uu.saw_end)
        .unwrap_or(true);
    assert!(!state_before, "no end marker has been seen yet");

    // Final part arrives without `ended`.
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;

    // The file completed, so its uu state was closed out and the damage
    // reported. The entry survives as a tombstone — it is what keeps a restart
    // checkpoint from ever being written for a uuencode file — but it holds no
    // bytes. Nothing may claim a verified reading of the file either: every
    // uuencode segment commits with part_crc_verified false.
    let closed = pipeline.uu_files.get(&file_id).expect("uu tombstone");
    assert!(closed.finished);
    assert!(closed.parked.is_empty());
    assert!(pipeline.block_crc_verdicts(file_id).is_none());
}

/// Number of segments queued for download on a job.
fn queued_segment_count(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.download_queue.len())
        .unwrap_or(0)
}

#[tokio::test]
async fn uu_park_displacement_requeues_instead_of_losing_the_segment() {
    // A displaced part's bytes are dropped, and the download layer already
    // counts that segment as delivered — so without a fresh fetch its data
    // exists nowhere and the cursor wedges forever when it reaches that
    // ordinal. The displacement must put it back on the queue.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_buf_max_pending = 1; // force displacement on the second park
    let job_id = JobId(20068);
    let parts: Vec<Vec<u8>> = (0..4u8).map(|i| vec![b'a' + i; 120 + i as usize]).collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let queued_before = queued_segment_count(&pipeline, job_id);

    // Parts 1, 2 and 3 all arrive before part 0. With a one-slot park, two of
    // them must be displaced.
    for index in [1u32, 2, 3] {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index == 3,
        )
        .await;
    }
    assert!(
        queued_segment_count(&pipeline, job_id) > queued_before,
        "displaced parts must be requeued for download, not dropped"
    );
    // Retry budget is untouched: park pressure is an ordering condition.
    assert!(
        pipeline.decode_retries.is_empty(),
        "displacement must not spend any segment's retry budget"
    );

    // Their re-fetches arrive, then the prefix, and the file completes whole.
    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    for index in [1u32, 2, 3] {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index == 3,
        )
        .await;
    }

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(
        written,
        parts.concat(),
        "no wedge: every part found its home"
    );
}

#[tokio::test]
async fn uu_park_pressure_storm_fails_nothing() {
    // Many parts arriving far ahead of a slow lowest ordinal is an ordering
    // pathology, not corruption. It must not manufacture a single permanent
    // failure, and the file must still assemble byte-identically.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_buf_max_pending = 2;
    let job_id = JobId(20069);
    let parts: Vec<Vec<u8>> = (0..12u8).map(|i| vec![b'a' + i; 60 + i as usize]).collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Storm: every part except 0, repeatedly, in descending order.
    for _ in 0..3 {
        for index in (1..parts.len() as u32).rev() {
            submit_uu_segment(
                &mut pipeline,
                file_id,
                index,
                &parts[index as usize],
                false,
                index as usize == parts.len() - 1,
            )
            .await;
        }
    }
    assert!(
        pipeline.decode_retries.is_empty(),
        "an ordering storm must never charge retry budget"
    );
    assert!(pipeline.jobs.contains_key(&job_id), "the job must survive");

    // Now drain in order; every ordinal places.
    for index in 0..parts.len() as u32 {
        submit_uu_segment(
            &mut pipeline,
            file_id,
            index,
            &parts[index as usize],
            false,
            index as usize == parts.len() - 1,
        )
        .await;
    }

    let written = tokio::fs::read(working_dir.join("silver-horizon.bin"))
        .await
        .unwrap();
    assert_eq!(written, parts.concat());
}

#[tokio::test]
async fn uu_stale_arrival_after_a_shift_is_dropped_without_retry() {
    // Once the cursor shifts past a booked-failed ordinal, a late copy of that
    // article has no home and never will. Re-fetching it would fetch bytes that
    // can never be placed, so it is dropped terminally.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20070);
    let parts: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'a' + i; 90 + i as usize]).collect();
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_uu_segment(&mut pipeline, file_id, 0, &parts[0], false, false).await;
    pipeline.skip_failed_uu_segment(SegmentId {
        file_id,
        segment_number: 1,
    });
    let damaged_before = pipeline.uu_files.get(&file_id).map(|uu| uu.damaged);
    let queued_before = queued_segment_count(&pipeline, job_id);

    // Part 1 shows up anyway, from a server that was slow rather than missing.
    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;

    assert_eq!(
        queued_segment_count(&pipeline, job_id),
        queued_before,
        "a stale arrival must not be requeued"
    );
    assert!(
        pipeline.decode_retries.is_empty(),
        "a stale arrival must not charge retry budget"
    );
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.damaged),
        damaged_before,
        "damage state is unchanged by a stale arrival"
    );
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.next_index),
        Some(2),
        "the cursor must not move for a stale arrival"
    );
}

#[tokio::test]
async fn uu_file_never_records_a_restart_checkpoint_floor() {
    // The restart checkpoint is a count of DECODED bytes, and the restore path
    // that reads it back walks the NZB's DECLARED (encoded) segment sizes to
    // decide which ordinals those bytes cover. yEnc declares ~1.03x its decoded
    // bytes, so the walk merely stops a segment early. uuencode declares
    // ~1.38x, so it would mark a prefix of ordinals received whose decoded end
    // nobody can compute — and a uuencode part's offset IS the decoded length
    // of its whole prefix. The resumed cursor would sit at ordinal 0 with every
    // arriving part parked behind ordinals that were never queued.
    //
    // So a uuencode file writes no checkpoint at all, for its final write as
    // much as for every write before it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20071);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 900], vec![b'b'; 800], vec![b'c'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for (index, part) in parts.iter().enumerate() {
        let last = index == parts.len() - 1;
        submit_uu_segment(&mut pipeline, file_id, index as u32, part, false, last).await;
        assert!(
            !pipeline.pending_file_progress.contains_key(&file_id),
            "a uuencode file must never record a restart checkpoint floor"
        );
    }
    assert!(!pipeline.persisted_file_progress.contains_key(&file_id));

    // Control: the same seam on the yEnc path still records one, so the
    // suppression above is specific to uuencode rather than a dead checkpoint.
    let yenc_job = JobId(20072);
    let payload = vec![b'y'; 512];
    insert_active_job(
        &mut pipeline,
        yenc_job,
        standalone_job_spec("Silver Horizon YEnc", &[("silver-horizon.bin".into(), 528)]),
    )
    .await;
    let yenc_file = NzbFileId {
        job_id: yenc_job,
        file_index: 0,
    };
    submit_decoded_segment(
        &mut pipeline,
        yenc_file,
        0,
        0,
        &payload,
        "silver-horizon.bin",
        None,
    )
    .await;
    assert_eq!(
        pipeline.pending_file_progress.get(&yenc_file).copied(),
        Some(payload.len() as u64),
        "a yEnc file still records its checkpoint floor"
    );
}

#[tokio::test]
async fn uu_begin_name_from_the_opening_part_survives_to_completion() {
    // yEnc repeats `name=` on every article, so the article that completes a
    // file carries the name to the identity seam. uuencode states it once, on
    // the part that opens the body — normally the first, never the last. The
    // name is retained per file so a uuencode file reaches the same identity
    // and rebind reasoning a yEnc file does.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20073);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 700], vec![b'b'; 500], vec![b'c'; 300]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Only the opening part carries a `begin` header; the rest are bare data.
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        0,
        &parts[0],
        false,
        false,
        "silver-horizon.bin",
    )
    .await;
    submit_uu_segment_named(&mut pipeline, file_id, 1, &parts[1], false, false, "").await;
    submit_uu_segment_named(&mut pipeline, file_id, 2, &parts[2], false, true, "").await;

    let closed = pipeline.uu_files.get(&file_id).expect("uu tombstone");
    assert!(closed.finished, "the file completed");
    assert_eq!(
        closed.filename.as_deref(),
        Some("silver-horizon.bin"),
        "the begin-line name must still be available when the last, nameless part completes the file"
    );
}

#[tokio::test]
async fn uu_begin_name_registers_identity_even_when_the_part_is_parked() {
    // The name-carrying part is a part like any other: it can arrive ahead of
    // its prefix and be parked, and the placement branch returns early when it
    // is. Identity must be recorded before that decision, not after it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20074);
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 400], vec![b'b'; 400]];
    insert_active_job(
        &mut pipeline,
        job_id,
        uu_job_spec(&parts.iter().map(|p| p.len()).collect::<Vec<_>>()),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Ordinal 1 arrives first and parks, and it is the one carrying the name —
    // which the reference decoders apply regardless of which part states it.
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        1,
        &parts[1],
        false,
        true,
        "silver-horizon.vol000+01.par2",
    )
    .await;
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
        Some(1),
        "the part is parked, so its placement branch returned early"
    );
    assert_eq!(
        pipeline
            .uu_files
            .get(&file_id)
            .and_then(|uu| uu.filename.as_deref()),
        Some("silver-horizon.vol000+01.par2"),
        "the name was retained before the placement decision"
    );
    // And the same seam the yEnc path uses for PAR2 recovery-count registration
    // ran too, rather than being skipped along with the placement.
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&0))
            .map(|file| file.recovery_blocks),
        Some(1),
    );
}

/// A job carrying both encodings: yEnc files whose declared segment sizes run
/// ~1.03x their decoded bytes, and one uuencode file whose declared sizes run
/// ~1.38x — exactly as the two encodings appear in a real NZB, since the
/// `<segment bytes>` attribute is always the ENCODED figure.
fn mixed_encoding_job_spec(
    yenc_files: &[(&str, &[usize])],
    uu_file: (&str, &[usize]),
) -> (JobSpec, Vec<u32>, u32) {
    let mut files = Vec::new();
    let mut yenc_indices = Vec::new();

    for (filename, decoded_sizes) in yenc_files {
        yenc_indices.push(files.len() as u32);
        let index = files.len();
        files.push(FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: decoded_sizes
                .iter()
                .enumerate()
                .map(|(ordinal, decoded)| {
                    segment_spec! {
                        number: ordinal as u32,
                        bytes: (*decoded as f64 * 1.03).ceil() as u32,
                        message_id: format!("mixed-y{index}-{ordinal}@example.com"),
                    }
                })
                .collect(),
        });
    }

    let uu_index = files.len() as u32;
    files.push(FileSpec {
        filename: uu_file.0.to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: uu_file
            .1
            .iter()
            .enumerate()
            .map(|(ordinal, decoded)| {
                segment_spec! {
                    number: ordinal as u32,
                    bytes: (*decoded as f64 * 1.38).ceil() as u32,
                    message_id: format!("mixed-u-{ordinal}@example.com"),
                }
            })
            .collect(),
    });

    let spec = JobSpec {
        name: "Silver Horizon Mixed".to_string(),
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
    (spec, yenc_indices, uu_index)
}

#[tokio::test]
async fn a_mixed_yenc_and_uuencode_job_writes_every_file_byte_identically() {
    // One NZB, two encodings. The two placement rules are completely different
    // — a yEnc part states its own offset, a uuencode part's offset is the
    // cumulative DECODED length of its prefix — and the two files' declared
    // sizes are wrong by different factors (1.03x against 1.38x). This is the
    // shape where a leaked declared byte count in either rule scatters exactly
    // one of the files while the other still looks fine.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20080);

    let yenc_a: Vec<Vec<u8>> = vec![vec![b'A'; 1500], vec![b'B'; 1500], vec![b'C'; 640]];
    let yenc_b: Vec<Vec<u8>> = vec![vec![b'D'; 900], vec![b'E'; 375]];
    let uu: Vec<Vec<u8>> = vec![vec![b'u'; 1100], vec![b'v'; 1100], vec![b'w'; 425]];

    let (spec, yenc_indices, uu_index) = mixed_encoding_job_spec(
        &[
            (
                "silver-horizon.s01e01.mkv",
                &yenc_a.iter().map(Vec::len).collect::<Vec<_>>(),
            ),
            (
                "silver-horizon.s01e02.mkv",
                &yenc_b.iter().map(Vec::len).collect::<Vec<_>>(),
            ),
        ],
        (
            "silver-horizon.readme.txt",
            &uu.iter().map(Vec::len).collect::<Vec<_>>(),
        ),
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let file_a = NzbFileId {
        job_id,
        file_index: yenc_indices[0],
    };
    let file_b = NzbFileId {
        job_id,
        file_index: yenc_indices[1],
    };
    let file_uu = NzbFileId {
        job_id,
        file_index: uu_index,
    };

    // Interleaved arrival across all three files, with each file's own parts out
    // of order where its rules allow it: the yEnc files take arbitrary order,
    // the uuencode file's early part parks until its prefix lands.
    submit_uu_segment_named(
        &mut pipeline,
        file_uu,
        0,
        &uu[0],
        false,
        false,
        "silver-horizon.readme.txt",
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        2,
        (yenc_a[0].len() + yenc_a[1].len()) as u64,
        &yenc_a[2],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;
    submit_uu_segment_named(&mut pipeline, file_uu, 2, &uu[2], false, true, "").await;
    submit_decoded_segment(
        &mut pipeline,
        file_b,
        1,
        yenc_b[0].len() as u64,
        &yenc_b[1],
        "silver-horizon.s01e02.mkv",
        None,
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        0,
        0,
        &yenc_a[0],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;
    // Releases the parked uuencode part behind it in the same call.
    submit_uu_segment_named(&mut pipeline, file_uu, 1, &uu[1], false, false, "").await;
    submit_decoded_segment(
        &mut pipeline,
        file_b,
        0,
        0,
        &yenc_b[0],
        "silver-horizon.s01e02.mkv",
        None,
    )
    .await;
    submit_decoded_segment(
        &mut pipeline,
        file_a,
        1,
        yenc_a[0].len() as u64,
        &yenc_a[1],
        "silver-horizon.s01e01.mkv",
        None,
    )
    .await;

    for (file_id, name, expected) in [
        (file_a, "silver-horizon.s01e01.mkv", yenc_a.concat()),
        (file_b, "silver-horizon.s01e02.mkv", yenc_b.concat()),
        (file_uu, "silver-horizon.readme.txt", uu.concat()),
    ] {
        assert!(
            pipeline
                .jobs
                .get(&job_id)
                .and_then(|state| state.assembly.file(file_id))
                .is_some_and(|file| file.is_complete()),
            "{name} did not complete"
        );
        let written = tokio::fs::read(working_dir.join(name)).await.unwrap();
        assert_eq!(written, expected, "{name} was not written byte-identically");
    }

    // The two encodings kept their own evidence rules: the yEnc files each
    // proved a contiguous decoded tiling, the uuencode file fed the grid
    // nothing and recorded no restart checkpoint.
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        state
            .assembly
            .file(file_a)
            .unwrap()
            .contiguous_placements_proven()
    );
    assert!(
        state
            .assembly
            .file(file_b)
            .unwrap()
            .contiguous_placements_proven()
    );
    assert!(pipeline.block_crc_verdicts(file_uu).is_none());
    assert!(!pipeline.pending_file_progress.contains_key(&file_uu));
    assert!(pipeline.pending_file_progress.contains_key(&file_a));
}

// ---- PAR2 binding by content (obfuscation) ----

/// A job holding one file under `posted_name`, with a recovery set describing
/// `described` files by their real names, and `prefix` captured for file 0.
///
/// The posted name and the described names are deliberately free to disagree:
/// that disagreement is what an obfuscated post *is*.
async fn obfuscated_binding_fixture(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posted_name: &str,
    described: &[(&str, &[u8])],
    prefix: &[u8],
) -> (Pipeline, NzbFileId) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Silver Horizon", &[(posted_name.to_string(), 4096)]),
    )
    .await;
    let par2_set = build_repairable_par2_set_for_files(described, 1024, 1);
    let set_id = par2_set.recovery_set_id;
    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.served = Some(set_id);
    runtime.ensure_set_runtime(set_id).set = Some(std::sync::Arc::new(par2_set));
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline.file_prefix_16k.insert(file_id, prefix.to_vec());
    (pipeline, file_id)
}

/// A payload whose first 16 KiB are distinctive, so a content match is a real
/// match rather than an accident of everything being zeros.
fn binding_payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn a_complete_content_match_with_the_wrong_length_is_refused() {
    let payload = binding_payload(29, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20110),
        "c841ef20.bin",
        &[("silver-horizon.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        &payload[..4_096],
        "c841ef20.bin",
        None,
    )
    .await;
    pipeline.file_prefix_16k.insert(
        file_id,
        payload[..crate::pipeline::PAR2_HASH_16K_BYTES].to_vec(),
    );
    pipeline.file_declared_size.remove(&file_id);

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(file.is_complete(), "the fixture must be complete");
    assert_ne!(file.received_bytes(), payload.len() as u64);
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a complete file with contradictory decoded length must not content-bind"
    );
}

#[tokio::test]
async fn an_incomplete_content_match_at_or_under_the_described_length_still_binds() {
    let payload = binding_payload(31, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20111),
        "9d2a18ce.bin",
        &[("silver-horizon.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(!file.is_complete(), "the fixture must remain incomplete");
    assert!(file.received_bytes() <= payload.len() as u64);
    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_some(),
        "in-stream verification needs incomplete, noncontradictory files to bind"
    );
}

#[tokio::test]
async fn an_incomplete_fragment_with_a_contradictory_declared_size_is_refused() {
    let payload = binding_payload(37, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20112),
        "onyx-prairie.mkv.001",
        &[("onyx-prairie.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;
    pipeline.file_declared_size.insert(file_id, 16_384);

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a contradictory yEnc declaration must refuse the content match"
    );
}

#[tokio::test]
async fn an_incomplete_obfuscated_file_with_a_contradictory_declared_size_is_refused() {
    let payload = binding_payload(39, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20115),
        "7e4c19ab.bin",
        &[("onyx-prairie.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;
    pipeline.file_declared_size.insert(file_id, 16_384);

    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("test file");
    assert!(!file.is_complete(), "the fixture must remain incomplete");
    assert!(file.received_bytes() <= payload.len() as u64);
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a contradictory yEnc declaration must refuse an otherwise matching content bind"
    );
}

#[tokio::test]
async fn an_absent_declared_size_does_not_change_content_binding() {
    let payload = binding_payload(41, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20113),
        "b0d0c6aa.bin",
        &[("ivory-meadow.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_some(),
        "without a declared size, a matching prefix remains usable"
    );
}

#[tokio::test]
async fn a_split_fragment_prefix_match_is_refused() {
    let payload = binding_payload(43, 49_152);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20114),
        "ivory-meadow.mkv.001",
        &[("ivory-meadow.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    assert!(!pipeline.file_declared_size.contains_key(&file_id));
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a numeric split fragment cannot bind to the joined description by prefix"
    );
}

#[tokio::test]
async fn an_obfuscated_file_binds_to_its_description_by_content() {
    // The point of the whole seam. The post says `a7f3e91c8b2d.bin`; the
    // recovery set describes `silver-horizon.s01e01.mkv`. Nothing matches by
    // name, so before this the file bound to no description at all — and an
    // unbound file has nothing to measure its in-stream block verdicts against,
    // so the entire dual-CRC grid lapsed for it.
    let payload = binding_payload(7, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20090),
        "a7f3e91c8b2d.bin",
        &[("silver-horizon.s01e01.mkv", &payload)],
        &payload[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = *set.files.keys().next().expect("one description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected),
        "the bytes are what the obfuscation did not touch"
    );
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.described_length),
        Some(payload.len() as u64),
        "and the binding carries the DESCRIBED length, never a declared one"
    );
}

#[tokio::test]
async fn content_binding_disambiguates_equal_length_obfuscated_descriptions() {
    let first = binding_payload(73, 40_000);
    let second = binding_payload(74, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(200901),
        "a7f3e91c8b2d.bin",
        &[
            ("silver-horizon.s01e01.mkv", &first),
            ("silver-horizon.s01e02.mkv", &second),
        ],
        &first[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = set
        .files
        .iter()
        .find_map(|(id, description)| {
            (description.filename == "silver-horizon.s01e01.mkv").then_some(*id)
        })
        .expect("first description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected),
        "the hash identifies one description even though length alone is ambiguous"
    );
}

#[tokio::test]
async fn a_short_description_binds_from_its_whole_file_hash() {
    // The spec trap. A description shorter than 16 KiB hashes its whole file
    // with no padding, so a matcher that demanded 16 KiB of prefix — or that
    // padded the short one out — would refuse exactly the small files an
    // obfuscated set tends to open with.
    let payload = binding_payload(19, 5_000);
    assert!(payload.len() < crate::pipeline::PAR2_HASH_16K_BYTES);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20091),
        "3f92aa10.bin",
        &[("silver-horizon.nfo", &payload)],
        &payload,
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let expected = *set.files.keys().next().expect("one description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(expected)
    );
}

#[tokio::test]
async fn two_descriptions_sharing_a_prefix_fail_closed() {
    // Volumes of one set routinely open with identical headers, so a shared
    // 16 KiB prefix is a real shape rather than a contrived one. Binding either
    // way would be a guess; the file stays unbound and is read at completion,
    // exactly as every file was before the grid existed.
    let shared = binding_payload(3, crate::pipeline::PAR2_HASH_16K_BYTES);
    let mut first = shared.clone();
    first.extend_from_slice(&binding_payload(101, 4_096));
    let mut second = shared.clone();
    second.extend_from_slice(&binding_payload(202, 4_096));

    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20092),
        "bb17c40e.bin",
        &[
            ("silver-horizon.part01.rar", &first),
            ("silver-horizon.part02.rar", &second),
        ],
        &shared,
    )
    .await;

    // Non-vacuity: the two descriptions really do share the hash being matched.
    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let hashes: HashSet<[u8; 16]> = set.files.values().map(|desc| desc.hash_16k).collect();
    assert_eq!(
        hashes.len(),
        1,
        "the fixture must produce one shared hash_16k, or nothing is ambiguous"
    );

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "an ambiguous content match must refuse, on the same terms an ambiguous \
         name match always has"
    );
}

#[tokio::test]
async fn a_name_match_still_wins_over_content() {
    // Content binding is the fallback and must never become the rule: it is
    // consulted only when no description answers to any of the file's names.
    // Here the posted name matches one description while the captured bytes
    // match a different one, and the name has to win.
    let named = binding_payload(11, 20_000);
    let other = binding_payload(77, 20_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20093),
        "silver-horizon.s01e02.mkv",
        &[
            ("silver-horizon.s01e02.mkv", &named),
            ("amber-trail.s01e01.mkv", &other),
        ],
        // The bytes of the OTHER description.
        &other[..crate::pipeline::PAR2_HASH_16K_BYTES],
    )
    .await;

    let set = pipeline.par2_set(file_id.job_id).cloned().expect("a set");
    let by_name = *set
        .files
        .iter()
        .find(|(_, desc)| desc.filename == "silver-horizon.s01e02.mkv")
        .map(|(file_id, _)| file_id)
        .expect("the named description");
    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .map(|bound| bound.par2_file_id),
        Some(by_name),
        "the name path runs first and returns before any hashing happens"
    );
}

#[tokio::test]
async fn a_file_whose_first_article_never_arrived_never_content_binds() {
    // The capture is anchored at offset 0 and grows only from its own end, so a
    // file missing its opening article has no prefix at all — and a file with a
    // hole in its prefix would be hashing bytes that are not the file's first
    // bytes. Both answer "no binding" rather than guessing, and neither panics.
    let payload = binding_payload(23, 40_000);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, file_id) = obfuscated_binding_fixture(
        &temp_dir,
        JobId(20094),
        "c81be004.bin",
        &[("silver-horizon.s01e03.mkv", &payload)],
        &[],
    )
    .await;
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "an empty capture binds nothing"
    );

    // No entry at all — the shape when the first article is still outstanding.
    pipeline.file_prefix_16k.remove(&file_id);
    assert!(pipeline.resolve_par2_file_binding(file_id).is_none());

    // A capture that is real but shorter than the description's window: the
    // second article landed, the first did not, and the buffer refused to
    // stitch it in at its offset.
    pipeline
        .file_prefix_16k
        .insert(file_id, payload[..1_000].to_vec());
    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a partial prefix cannot be tested against a 16 KiB window without \
         inventing the bytes it is missing"
    );
}

#[tokio::test]
async fn the_prefix_capture_takes_only_an_offset_zero_anchored_run() {
    // The capture rule itself, at the seam. Articles arriving out of order must
    // not leave a stitched-together buffer that hashes to nothing real.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20095);
    let parts: Vec<Vec<u8>> = vec![
        binding_payload(1, 1_000),
        binding_payload(2, 1_000),
        binding_payload(3, 1_000),
    ];
    let declared: Vec<u32> = parts.iter().map(|part| part.len() as u32 + 32).collect();
    let mut spec = standalone_job_spec("Silver Horizon Prefix", &[("obf.bin".into(), declared[0])]);
    spec.files[0].segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        spec.files[0].segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("pfx-{index}@example.com"),
        });
    }
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Segment 1 lands first: it starts past offset 0, so nothing is captured.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        parts[0].len() as u64,
        &parts[1],
        "obf.bin",
        None,
    )
    .await;
    assert!(
        pipeline
            .file_prefix_16k
            .get(&file_id)
            .is_none_or(Vec::is_empty),
        "a span that does not start at the captured end is skipped, never stitched"
    );

    // Segment 0 lands and opens the run.
    submit_decoded_segment(&mut pipeline, file_id, 0, 0, &parts[0], "obf.bin", None).await;
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::len),
        Some(parts[0].len()),
        "the run starts at zero"
    );

    // Segment 2 extends it; the hole segment 1 left is never closed, because
    // the capture only ever grows from its own end.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        2,
        (parts[0].len() + parts[1].len()) as u64,
        &parts[2],
        "obf.bin",
        None,
    )
    .await;
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::len),
        Some(parts[0].len()),
        "and a later span past the hole cannot extend it either"
    );
    assert_eq!(
        pipeline.file_prefix_16k.get(&file_id).map(Vec::as_slice),
        Some(parts[0].as_slice()),
        "what was captured is exactly the file's own first bytes"
    );
}
