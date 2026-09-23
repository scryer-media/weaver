//! `download_dispatch` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn emits_download_pipeline_drained_once_before_later_completion_events() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(20011);
    insert_active_job(
        &mut pipeline,
        job_id,
        JobSpec {
            name: "Drain Pending Buffers".to_string(),
            password: None,
            files: vec![],
            total_bytes: 0,
            category: None,
            metadata: vec![],
        },
    )
    .await;

    pipeline.active_download_passes.insert(job_id);
    pipeline.pending_decode.push_back(PendingDecodeWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        raw: Bytes::from_static(b"pending"),
        source_server_idx: None,
        exclude_servers: Vec::new(),
    });

    pipeline.emit_download_finished_if_active(job_id);
    assert!(pipeline.jobs_finalizing_download.contains(&job_id));

    pipeline.pending_decode.clear();
    pipeline.schedule_job_completion_check(job_id);
    pump_pipeline_runtime_queues(&mut pipeline).await;

    let drained_events = drain_job_events(&mut events, job_id);
    assert!(matches!(
        drained_events.first(),
        Some(PipelineEvent::DownloadFinished {
            finalization_pending: true,
            ..
        })
    ));
    assert_eq!(
        drained_events
            .iter()
            .filter(|event| matches!(event, PipelineEvent::DownloadPipelineDrained { .. }))
            .count(),
        1
    );
    let drained_idx = drained_events
        .iter()
        .position(|event| matches!(event, PipelineEvent::DownloadPipelineDrained { .. }))
        .expect("download pipeline drained event");
    let next_stage_idx = drained_events
        .iter()
        .position(|event| {
            matches!(
                event,
                PipelineEvent::MoveToCompleteStarted { .. }
                    | PipelineEvent::JobVerificationStarted { .. }
                    | PipelineEvent::RepairStarted { .. }
                    | PipelineEvent::ExtractionReady { .. }
                    | PipelineEvent::JobCompleted { .. }
            )
        })
        .expect("later completion event");
    assert!(drained_idx < next_stage_idx, "events: {drained_events:?}");
    assert!(!pipeline.jobs_finalizing_download.contains(&job_id));
}

#[tokio::test]
async fn skips_download_pipeline_drained_when_no_post_download_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(20012);
    insert_active_job(
        &mut pipeline,
        job_id,
        JobSpec {
            name: "No Drain Gap".to_string(),
            password: None,
            files: vec![],
            total_bytes: 0,
            category: None,
            metadata: vec![],
        },
    )
    .await;

    pipeline.active_download_passes.insert(job_id);
    pipeline.emit_download_finished_if_active(job_id);
    pipeline.schedule_job_completion_check(job_id);
    pump_pipeline_runtime_queues(&mut pipeline).await;

    let drained_events = drain_job_events(&mut events, job_id);
    assert!(matches!(
        drained_events.first(),
        Some(PipelineEvent::DownloadFinished {
            finalization_pending: false,
            ..
        })
    ));
    assert!(
        drained_events
            .iter()
            .all(|event| !matches!(event, PipelineEvent::DownloadPipelineDrained { .. })),
        "events: {drained_events:?}"
    );
    assert!(!pipeline.jobs_finalizing_download.contains(&job_id));
}

#[tokio::test]
async fn dispatch_downloads_respects_hard_decode_byte_pressure() {
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
    let job_id = JobId(20002);
    let files = vec![("queued.bin".to_string(), 512u32)];
    let spec = standalone_job_spec("Decode Byte Pressure", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.decode_backlog_budget_bytes = 1024;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(1024, Ordering::Relaxed);
    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_stalls_total
            .load(Ordering::Relaxed),
        1
    );
    assert!(pipeline.download_pressure_hard_stall_started_at.is_some());
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
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        files.len()
    );

    tokio::time::sleep(Duration::from_millis(2)).await;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(0, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert!(pipeline.download_pressure_hard_stall_started_at.is_none());
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_current_stall_ms
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        pipeline
            .metrics
            .download_pressure_stall_duration_ms
            .load(Ordering::Relaxed)
            > 0
    );
}

#[tokio::test]
async fn dispatch_downloads_waits_for_downstream_work_when_restart_durable_lead_is_too_large() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        2,
    )
    .await;
    let job_id = JobId(20003);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let durable_lead_limit = Pipeline::restart_durable_lead_limit_bytes();
    let segment_bytes = 1024 * 1024u32;
    let segment_count = (durable_lead_limit / u64::from(segment_bytes) + 2) as u32;
    let segments = (0..segment_count)
        .map(|segment| {
            segment_spec! {
                number: segment,
                bytes: segment_bytes,
                message_id: format!("restart-lead-{segment}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "Restart Durable Lead".to_string(),
        password: None,
        total_bytes: u64::from(segment_count) * u64::from(segment_bytes),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "large.bin".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments,
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.downloaded_bytes = durable_lead_limit;
        state.restored_download_floor_bytes = 1;
    }
    let next_work = DownloadWork {
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        message_id: MessageId::new("restart-lead-0@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: FileRole::Standalone.download_priority(),
        byte_estimate: segment_bytes,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: vec![],
        avoid_server: None,
    };

    pipeline.active_decodes_by_job.insert(job_id, 1);
    assert!(!pipeline.primary_download_within_restart_durable_lead(job_id, &next_work));

    let blocked_pass_started = Instant::now();
    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        segment_count as usize
    );
    assert_eq!(
        pipeline
            .metrics
            .download_restart_durable_lead_blocked_total
            .load(Ordering::Relaxed),
        1
    );
    assert!(
        pipeline.last_download_dispatch_stall_log_at.is_some(),
        "durable-lead parking with queued work and no active downloads should emit a liveness diagnostic"
    );
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .get(&job_id)
            .is_some_and(|ready_at| *ready_at > blocked_pass_started)
    );
    let retry_delay = pipeline
        .next_restart_durable_lead_retry_delay()
        .expect("durable lead block should expose a retry wakeup");
    assert!(retry_delay <= Duration::from_millis(250));

    // The block holds for a fraction of a second. A pass inside it is the case
    // under test, so the test keeps the hold open rather than racing the
    // clock to arrive in time.
    *pipeline
        .download_restart_durable_lead_retry_after
        .get_mut(&job_id)
        .expect("the blocked job keeps its retry time") =
        Instant::now() + Duration::from_secs(3600);
    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline
            .metrics
            .download_restart_durable_lead_blocked_total
            .load(Ordering::Relaxed),
        1
    );

    pipeline
        .persisted_file_progress
        .insert(file_id, durable_lead_limit);
    pipeline.active_decodes_by_job.remove(&job_id);
    pipeline.note_released_download_result_pending(job_id, 768 * 1024);

    assert!(pipeline.primary_download_within_restart_durable_lead(job_id, &next_work));
    assert_eq!(
        pipeline
            .pending_released_download_result_bytes_by_job
            .get(&job_id)
            .copied(),
        Some(768 * 1024)
    );
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .get(&job_id)
            .is_some_and(|ready_at| *ready_at > Instant::now())
    );
    pipeline.dispatch_downloads();

    assert!(pipeline.active_downloads > 0);
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&job_id)
    );
}

#[tokio::test]
async fn hot_download_batch_lease_stays_large_for_fresh_job_when_restart_lead_would_cap_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        2,
    )
    .await;
    let job_id = JobId(20029);
    let durable_lead_limit = Pipeline::restart_durable_lead_limit_bytes();
    let segment_bytes = (durable_lead_limit / 4).max(1).min(u64::from(u32::MAX)) as u32;
    let segment_count = 6u32;
    let segments = (0..segment_count)
        .map(|segment| {
            segment_spec! {
                number: segment,
                bytes: segment_bytes,
                message_id: format!("restart-lead-lease-{segment}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "Restart Durable Lead Lease".to_string(),
        password: None,
        total_bytes: u64::from(segment_count) * u64::from(segment_bytes),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "large-lease.bin".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments,
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .pending_released_download_results_by_job
        .insert(job_id, 1);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, segment_count as usize);
    assert_eq!(
        pipeline.active_downloads_by_job.get(&job_id),
        Some(&(segment_count as usize))
    );
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 0);
}

#[tokio::test]
async fn hot_download_batch_lease_caps_restored_job_for_restart_durable_lead() {
    let temp_dir = tempfile::tempdir().unwrap();
    // Single connection on purpose: this pins the per-lease growth cap in
    // isolation. The durable-lead check on a *second* connection's first
    // item only has the coarse `estimated_undurable_download_bytes_for_job`
    // heuristic to go on (flat per-active-item, not real segment size), so a
    // wider fixture here would have a second lane slip past it — that
    // imprecision is real but belongs to the durable-lead accounting itself,
    // not to this test's job.
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        1,
    )
    .await;
    let job_id = JobId(20030);
    let durable_lead_limit = Pipeline::restart_durable_lead_limit_bytes();
    let segment_bytes = (durable_lead_limit / 4).max(1).min(u64::from(u32::MAX)) as u32;
    let segment_count = 6u32;
    let segments = (0..segment_count)
        .map(|segment| {
            segment_spec! {
                number: segment,
                bytes: segment_bytes,
                message_id: format!("restart-lead-restored-lease-{segment}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "Restart Durable Lead Restored Lease".to_string(),
        password: None,
        total_bytes: u64::from(segment_count) * u64::from(segment_bytes),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "restored-large-lease.bin".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments,
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .restored_download_floor_bytes = 1;
    pipeline
        .pending_released_download_results_by_job
        .insert(job_id, 1);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 4);
    assert_eq!(pipeline.active_downloads_by_job.get(&job_id), Some(&4));
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 2);
}

#[tokio::test]
async fn dispatch_downloads_escapes_restart_durable_lead_when_pipeline_is_idle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        2,
    )
    .await;
    let job_id = JobId(20030);
    let durable_lead_limit = Pipeline::restart_durable_lead_limit_bytes();
    let segment_bytes = 1024 * 1024u32;
    let segment_count = (durable_lead_limit / u64::from(segment_bytes) + 2) as u32;
    let segments = (0..segment_count)
        .map(|segment| {
            segment_spec! {
                number: segment,
                bytes: segment_bytes,
                message_id: format!("restart-lead-idle-{segment}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "Restart Durable Lead Idle".to_string(),
        password: None,
        total_bytes: u64::from(segment_count) * u64::from(segment_bytes),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "large-idle.bin".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments,
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        64,
        b"buffered-gap",
        "large-idle.bin",
        None,
    )
    .await;
    assert!(
        pipeline
            .write_buffers
            .get(&file_id)
            .is_some_and(|write_buf| write_buf.buffered_len() > 0)
    );
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = durable_lead_limit;
    pipeline
        .pending_released_download_results_by_job
        .insert(job_id, 1);
    pipeline
        .download_restart_durable_lead_retry_after
        .insert(job_id, Instant::now() + Duration::from_secs(3600));

    pipeline.dispatch_downloads();

    assert!(pipeline.active_downloads > 0);
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&job_id)
    );
    assert_eq!(
        pipeline
            .metrics
            .download_restart_durable_lead_blocked_total
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn dispatch_downloads_counts_completed_files_for_restart_durable_lead() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        2,
    )
    .await;
    let job_id = JobId(20004);
    let complete_files = 17u32;
    let file_bytes = 64 * 1024 * 1024u32;
    let mut files = Vec::new();
    for file_index in 0..=complete_files {
        files.push(FileSpec {
            filename: format!("part{file_index:02}.rar"),
            role: FileRole::RarVolume {
                volume_number: file_index,
            },
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: file_bytes,
                message_id: format!("small-complete-{file_index}@example.com"),
            }],
        });
    }
    let spec = JobSpec {
        name: "Restart Durable Lead Completed Files".to_string(),
        password: None,
        total_bytes: u64::from(complete_files + 1) * u64::from(file_bytes),
        category: None,
        metadata: vec![],
        files,
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        for file_index in 0..complete_files {
            let file_id = NzbFileId { job_id, file_index };
            state
                .assembly
                .file_mut(file_id)
                .unwrap()
                .commit_segment(0, file_bytes)
                .unwrap();
        }
        state.downloaded_bytes = u64::from(complete_files) * u64::from(file_bytes);
    }

    let next_file_id = NzbFileId {
        job_id,
        file_index: complete_files,
    };
    let next_work = DownloadWork {
        segment_id: SegmentId {
            file_id: next_file_id,
            segment_number: 0,
        },
        message_id: MessageId::new("small-complete-next@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: FileRole::RarVolume {
            volume_number: complete_files,
        }
        .download_priority(),
        byte_estimate: file_bytes,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: vec![],
        avoid_server: None,
    };
    assert!(pipeline.primary_download_within_restart_durable_lead(job_id, &next_work));
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .push(next_work);

    pipeline.dispatch_downloads();

    assert!(pipeline.active_downloads > 0);
    assert_eq!(
        pipeline
            .metrics
            .download_restart_durable_lead_blocked_total
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn dispatch_downloads_leaves_unpromoted_recovery_queue_parked_when_primary_queue_is_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20014);
    let spec = standalone_with_par2_job_spec("Recovery Dispatch", 128, 64);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        assert_eq!(state.recovery_queue.len(), 1);
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_recovery, 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().recovery_queue.len(), 1);
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
async fn dispatch_downloads_respects_hard_write_byte_pressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20004);
    let spec = standalone_job_spec(
        "Write Backlog Gates Dispatch",
        &[("queued.bin".to_string(), 512u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        damaged_source: None,
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 99,
        },
        decoded_size: 4096,
        data: DecodedChunk::from(vec![7u8; 4096]),
        part_crc: par2_rs::checksum::crc32(&vec![7u8; 4096]),
        part_crc_verified: true,
        declared_file_len: 0,
        yenc_name: "queued.bin".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments: Vec::new(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(4096, buffered);
    pipeline.note_write_buffered(buffered_len, 1);
    pipeline.write_backlog_budget_bytes = buffered_len;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_segments
            .load(Ordering::Relaxed),
        1
    );
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
}

#[tokio::test]
async fn capped_uu_spool_dispatches_only_the_missing_cursor_prefix() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20015);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "UU Spool Cursor Admission",
            &[("queued.bin".to_string(), 512)],
        ),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let work = |segment_number, message_id| DownloadWork {
        segment_id: SegmentId {
            file_id,
            segment_number,
        },
        message_id: MessageId::new(message_id),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 512,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    };
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    // The later segment is deliberately the queue head. Capped admission must
    // scan past it to fetch the ordinal that can release the parked prefix.
    state
        .download_queue
        .push(work(1, "uu-later@example.invalid"));
    state
        .download_queue
        .push(work(0, "uu-prefix@example.invalid"));
    pipeline.uu_files.insert(
        file_id,
        UuFileAssembly {
            next_index: 0,
            ..UuFileAssembly::default()
        },
    );
    pipeline.uu_spool_max_segments = 0;
    pipeline.uu_spool_available_bytes_for_test = Some(Some(u64::MAX));

    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("the cursor-closing prefix must still lease");

    assert_eq!(lease.works.len(), 1);
    assert_eq!(lease.works[0].segment_id.segment_number, 0);
    assert_eq!(pipeline.jobs[&job_id].download_queue.len(), 1);
    assert_eq!(
        pipeline.jobs[&job_id]
            .download_queue
            .peek_next_matching(|_| true)
            .map(|work| work.segment_id.segment_number),
        Some(1)
    );
}

#[tokio::test]
async fn capped_uu_spool_keeps_unrelated_yenc_ordinals_eligible() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let uu_job = JobId(20150);
    let yenc_job = JobId(20151);
    let mut spec = standalone_job_spec("Spool isolation", &[("small.bin".to_string(), 16)]);
    let mut second = spec.files[0].segments[0].clone();
    second.ordinal = 1;
    second.article_number = 2;
    second.message_id = "second@example.invalid".to_string();
    spec.files[0].segments.push(second);
    spec.total_bytes *= 2;
    insert_active_job(&mut pipeline, uu_job, spec.clone()).await;
    insert_active_job(&mut pipeline, yenc_job, spec).await;
    // Ordinal zero of each job is already out; only the tails stay queued, so
    // the UU job's remainder is the part the spool cursor holds.
    for job_id in [uu_job, yenc_job] {
        let first = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        assert_eq!(first.segment_id.segment_number, 0);
    }
    pipeline.uu_files.insert(
        NzbFileId {
            job_id: uu_job,
            file_index: 0,
        },
        UuFileAssembly::default(),
    );
    pipeline.uu_spool_available_bytes_for_test = Some(Some(0));
    assert!(pipeline.uu_spool_admission_capped(0));
    assert_eq!(
        pipeline.current_hot_job(),
        Some(uu_job),
        "the capped UU job is still the hot one; the walk must step past it"
    );
    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("yEnc beyond ordinal zero must keep progressing during UU disk pressure");
    assert_eq!(lease.job_id, yenc_job);
    assert_eq!(lease.works[0].segment_id.segment_number, 1);
    assert_eq!(lease.works[0].segment_id.file_id.job_id, yenc_job);
}

#[tokio::test]
async fn refresh_download_pressure_reports_combined_hard_byte_pressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline.write_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(1000, Ordering::Relaxed);
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
        DownloadPressureReason::DecodeAndWrite.as_code()
    );
}

#[tokio::test]
async fn refresh_download_pressure_counts_active_decode_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_active_bytes
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
}

#[tokio::test]
async fn refresh_download_pressure_combines_pending_active_and_released_result_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(100, Ordering::Relaxed);
    pipeline
        .metrics
        .decode_active_bytes
        .store(200, Ordering::Relaxed);
    pipeline
        .pending_released_download_result_bytes_by_job
        .insert(JobId(20027), 200);
    pipeline
        .pending_released_download_result_bytes_by_job
        .insert(JobId(20028), 200);

    pipeline.refresh_download_pressure();

    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Soft.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Decode.as_code()
    );

    pipeline
        .pending_released_download_result_bytes_by_job
        .insert(JobId(20028), 500);
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
}

#[tokio::test]
async fn refresh_download_pressure_saturates_released_result_byte_aggregation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(1, Ordering::Relaxed);
    pipeline
        .metrics
        .decode_active_bytes
        .store(1, Ordering::Relaxed);
    pipeline
        .pending_released_download_result_bytes_by_job
        .insert(JobId(20029), u64::MAX);
    pipeline
        .pending_released_download_result_bytes_by_job
        .insert(JobId(20030), 1);

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
}

#[tokio::test]
async fn released_result_bytes_block_dispatch_until_processing_clears_hysteresis() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;
    let queued_job_id = JobId(20031);
    insert_active_job(
        &mut pipeline,
        queued_job_id,
        standalone_job_spec(
            "Released Result Pressure",
            &[("queued.bin".to_string(), 512)],
        ),
    )
    .await;
    let released_job_id = JobId(20032);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: released_job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let payload = Bytes::from(vec![0; 1024]);
    pipeline.decode_backlog_budget_bytes = payload.len();
    pipeline.note_released_download_result_pending(released_job_id, payload.len() as u64);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );

    pipeline
        .process_released_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(payload)),
            attempts: vec![],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![],
            release_connection_slot: false,
        })
        .await;

    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert!(
        !pipeline
            .pending_released_download_result_bytes_by_job
            .contains_key(&released_job_id)
    );

    pipeline.dispatch_downloads();
    assert!(pipeline.active_downloads > 0);
}

#[tokio::test]
async fn dispatch_downloads_allows_postprocessing_status_with_remaining_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let cases = [
        (JobId(20006), JobStatus::Extracting),
        (JobId(20007), JobStatus::QueuedExtract),
    ];

    for (job_id, status) in &cases {
        insert_active_job(
            &mut pipeline,
            *job_id,
            standalone_job_spec(
                &format!("Postprocessing Dispatch {}", job_id.0),
                &[(format!("queued-{}.bin", job_id.0), 512u32)],
            ),
        )
        .await;
        let state = pipeline.jobs.get_mut(job_id).unwrap();
        state.status = status.clone();
        state.refresh_runtime_lanes_from_status();
        assert_eq!(
            state.download_state,
            crate::jobs::model::DownloadState::Complete
        );
        assert_eq!(state.download_queue.len(), 1);
    }

    pipeline.dispatch_downloads();

    // Both jobs dispatch: the second connection is not left idle once the
    // first job's single article has been handed out.
    assert_eq!(pipeline.active_downloads, 2);
    for (job_id, status) in &cases {
        let state = pipeline.jobs.get(job_id).unwrap();
        assert_eq!(&state.status, status);
        assert_eq!(state.download_queue.len(), 0);
        assert!(pipeline.active_download_passes.contains(job_id));
    }
}

#[tokio::test]
async fn dispatch_downloads_prefers_new_higher_priority_job_after_inflight_segments() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;

    let low_job_id = JobId(20015);
    let high_job_id = JobId(20016);

    insert_active_job(
        &mut pipeline,
        low_job_id,
        with_priority(
            standalone_job_spec(
                "Low Priority Active",
                &[
                    ("low-0.bin".to_string(), 512u32),
                    ("low-1.bin".to_string(), 512u32),
                ],
            ),
            "LOW",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        high_job_id,
        with_priority(
            standalone_job_spec("High Priority New", &[("high-0.bin".to_string(), 512u32)]),
            "HIGH",
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&low_job_id).unwrap();
        let _ = state
            .download_queue
            .pop()
            .expect("low-priority job should have an in-flight segment to drain");
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;
    pipeline.active_downloads_by_job.insert(low_job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(low_job_id, 1);
    pipeline.active_download_passes.insert(low_job_id);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 2);
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline.jobs.get(&low_job_id).unwrap().download_queue.len(),
        1
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&high_job_id)
            .unwrap()
            .download_queue
            .len(),
        0
    );
    assert_eq!(pipeline.active_downloads_by_job.get(&low_job_id), Some(&1));
    assert_eq!(pipeline.active_downloads_by_job.get(&high_job_id), Some(&1));
}

#[tokio::test]
async fn dispatch_downloads_yields_retained_hot_job_to_high_priority_while_critical_lane_runs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let normal_job_id = JobId(20029);
    let promoted_job_id = JobId(20030);

    insert_active_job(
        &mut pipeline,
        normal_job_id,
        with_priority(
            standalone_job_spec("Retained Normal", &many_standalone_files("normal", 64)),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        promoted_job_id,
        with_priority(
            standalone_job_spec("Promoted High", &[("promoted.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();
    assert_eq!(pipeline.current_hot_job(), Some(normal_job_id));
    let normal_queued_before_priority_update = pipeline
        .jobs
        .get(&normal_job_id)
        .unwrap()
        .download_queue
        .len();
    assert!(
        normal_queued_before_priority_update > 0,
        "the retained Normal job must still have queued ordinary work"
    );
    settle_lane_dials(&mut pipeline).await;

    // One lane is free while the prior completion-critical body remains active.
    // Existing work keeps running; the priority update owns only the new lane.
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;
    pipeline.active_downloads_by_job.insert(normal_job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(normal_job_id, 1);
    pipeline.active_downloads_by_file.clear();
    pipeline
        .active_completion_critical_connections_by_job
        .insert(normal_job_id, 1);
    pipeline
        .jobs
        .get_mut(&promoted_job_id)
        .unwrap()
        .spec
        .metadata = vec![("priority".to_string(), "HIGH".to_string())];

    assert_eq!(
        pipeline.current_hot_job(),
        Some(promoted_job_id),
        "the High update must replace the retained hot selection"
    );

    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline
            .jobs
            .get(&normal_job_id)
            .unwrap()
            .download_queue
            .len(),
        normal_queued_before_priority_update,
        "the Normal job's remaining work must not consume the new lane"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&promoted_job_id)
            .unwrap()
            .download_queue
            .len(),
        0,
        "the High job must receive the newly available lane"
    );
}

#[tokio::test]
async fn dispatch_downloads_prefers_earliest_job_within_priority_band() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    let earlier_job_id = JobId(20017);
    let later_job_id = JobId(20018);

    insert_active_job(
        &mut pipeline,
        earlier_job_id,
        with_priority(
            standalone_job_spec("Earlier Normal", &[("earlier.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        later_job_id,
        with_priority(
            standalone_job_spec(
                "Later More Progress",
                &[
                    ("later-0.bin".to_string(), 512u32),
                    ("later-1.bin".to_string(), 512u32),
                ],
            ),
            "NORMAL",
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&later_job_id).unwrap();
        let _ = state
            .download_queue
            .pop()
            .expect("later job should have one completed segment removed from queue");
        state
            .assembly
            .file_mut(NzbFileId {
                job_id: later_job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 512)
            .unwrap();
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(
        pipeline
            .jobs
            .get(&earlier_job_id)
            .unwrap()
            .download_queue
            .len(),
        0
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&later_job_id)
            .unwrap()
            .download_queue
            .len(),
        1
    );
}

#[tokio::test]
// A hot job with enough queued work to fill capacity takes every connection,
// even with same-priority peers waiting: a peer is only ever served once the
// hot job has nothing the asking server may fetch.
async fn dispatch_downloads_hot_job_takes_full_capacity_over_same_priority_peers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let job_ids = [JobId(21400), JobId(21401), JobId(21402), JobId(21403)];
    for (idx, job_id) in job_ids.into_iter().enumerate() {
        let files = many_standalone_files(&format!("bounded-share-{idx}"), 500);
        insert_active_job(
            &mut pipeline,
            job_id,
            with_priority(
                standalone_job_spec(&format!("Bounded Share {idx}"), &files),
                "NORMAL",
            ),
        )
        .await;
    }

    pipeline.dispatch_downloads();

    let hot_job_id = job_ids[0];
    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&8)
    );
    let peer_lane_total = job_ids[1..]
        .iter()
        .map(|job_id| {
            pipeline
                .active_download_connections_by_job
                .get(job_id)
                .copied()
                .unwrap_or(0)
        })
        .sum::<usize>();
    assert_eq!(peer_lane_total, 0);
}

/// Completion-critical work orders a job internally, never globally: a later,
/// lower-priority job whose whole queue is completion-critical recovery still
/// waits behind the hot job's ordinary bytes.
#[tokio::test]
async fn a_lower_priority_critical_recovery_waits_behind_the_hot_jobs_regular_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let hot_job_id = JobId(21420);
    let critical_job_id = JobId(21421);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Completion Critical Hot",
                &many_standalone_files("completion-critical-hot", 500),
            ),
            "HIGH",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        critical_job_id,
        with_priority(
            standalone_job_spec(
                "Completion Critical Recovery",
                &many_standalone_files("completion-critical-recovery", 500),
            ),
            "LOW",
        ),
    )
    .await;
    let critical_queued_before = {
        let state = pipeline.jobs.get_mut(&critical_job_id).unwrap();
        let work = state.download_queue.drain_all();
        for mut work in work {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
        state.download_queue.len()
    };

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&8)
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&critical_job_id),
        "no other job's critical work displaces the hot job's ordinary work"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&critical_job_id)
            .unwrap()
            .download_queue
            .len(),
        critical_queued_before
    );
}

#[tokio::test]
async fn the_hot_job_takes_the_only_connection_ahead_of_a_critical_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    let hot_job_id = JobId(21422);
    let critical_job_id = JobId(21423);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Single Lane Hot",
                &many_standalone_files("single-lane-hot", 10),
            ),
            "HIGH",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        critical_job_id,
        with_priority(
            standalone_job_spec(
                "Single Lane Critical",
                &many_standalone_files("single-lane-critical", 10),
            ),
            "LOW",
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&critical_job_id).unwrap();
        let work = state.download_queue.drain_all();
        for mut work in work {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&1)
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&critical_job_id)
    );
}

#[tokio::test]
async fn dispatch_downloads_single_job_keeps_hot_runway() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let job_id = JobId(21410);
    let files = many_standalone_files("single-hot-runway", 600);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Single Hot Runway", &files),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(job_id));
    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&job_id),
        Some(&8)
    );
    // Every connection took exactly one handout, sized by the refill want for
    // the depth its server runs at.
    let pressure = pipeline.refresh_download_pressure();
    let want =
        pipeline.download_refill_want(pipeline.download_lane_mode_for_server(0, pressure, true));
    assert_eq!(
        pipeline.active_downloads_by_job.get(&job_id),
        Some(&(8 * want))
    );
}

#[tokio::test]
async fn dispatch_downloads_does_not_share_with_lower_priority() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let hot_job_id = JobId(21420);
    let low_job_id = JobId(21421);
    let hot_files = many_standalone_files("normal-hot", 600);
    let low_files = many_standalone_files("low-peer", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(standalone_job_spec("Normal Hot", &hot_files), "NORMAL"),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        low_job_id,
        with_priority(standalone_job_spec("Low Peer", &low_files), "LOW"),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&8)
    );
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&low_job_id),
        None
    );
}

#[tokio::test]
async fn dispatch_downloads_under_soft_pressure_opens_one_lane_for_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let hot_job_id = JobId(21430);
    let peer_job_id = JobId(21431);
    let hot_files = many_standalone_files("pressure-hot", 600);
    let peer_files = many_standalone_files("pressure-peer", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(standalone_job_spec("Pressure Hot", &hot_files), "NORMAL"),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer_job_id,
        with_priority(standalone_job_spec("Pressure Peer", &peer_files), "NORMAL"),
    )
    .await;
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&peer_job_id),
        None
    );
}

/// Completion-critical work is a within-job ordering, so two later jobs made
/// entirely of it still wait while the hot job has regular bytes this server
/// can fetch.
#[tokio::test]
async fn critical_work_of_later_jobs_waits_behind_the_hot_jobs_regular_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;

    let hot_job_id = JobId(21500);
    let critical_a = JobId(21501);
    let critical_b = JobId(21502);

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Critical First Hot",
                // Comfortably more than four handouts' worth, so the pass is
                // capacity-bound rather than work-bound on the hot job.
                &many_standalone_files("critical-first-hot", 200),
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        critical_a,
        with_priority(
            standalone_job_spec(
                "Critical First A",
                &[("critical-a.bin".to_string(), 512u32)],
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        critical_b,
        with_priority(
            standalone_job_spec(
                "Critical First B",
                &[("critical-b.bin".to_string(), 512u32)],
            ),
            "NORMAL",
        ),
    )
    .await;
    for job_id in [critical_a, critical_b] {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        for mut work in state.download_queue.drain_all() {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    assert_eq!(pipeline.active_download_connections, 4);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&4)
    );
    assert_eq!(pipeline.active_completion_critical_connections, 0);
    for job_id in [critical_a, critical_b] {
        assert_eq!(
            pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
            1,
            "a later job's critical article waits while the hot job can be served"
        );
    }
}

/// New-contract test: with no connection ramp, the very first dispatch pass
/// reaches full configured capacity when there is enough queued work — no
/// gradual climb, no warmup.
#[tokio::test]
async fn first_dispatch_pass_reaches_full_capacity_with_no_ramp() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let job_id = JobId(21540);
    insert_active_job(
        &mut pipeline,
        job_id,
        // Comfortably more than eight handouts' worth, so the pass is
        // capacity-bound, not work-bound.
        standalone_job_spec("No Ramp", &many_standalone_files("no-ramp", 200)),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&job_id),
        Some(&8)
    );
}

#[tokio::test]
async fn hot_clear_pressure_lane_leases_sequential_runway() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    let job_id = JobId(20024);
    let files = (0..100)
        .map(|idx| (format!("hot-{idx}.bin"), 512u32))
        .collect::<Vec<_>>();
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Hot Sequential Runway", &files),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.current_hot_job(), Some(job_id));
    assert_eq!(pipeline.active_download_connections, 1);
    // The single lane took one handout; the rest of the job stays queued.
    let pressure = pipeline.refresh_download_pressure();
    let want =
        pipeline.download_refill_want(pipeline.download_lane_mode_for_server(0, pressure, true));
    assert_eq!(pipeline.active_downloads, want);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        100 - want
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_lease_items_total
            .load(Ordering::Relaxed),
        want as u64
    );
}

#[tokio::test]
// Hot fills everything: even when the hot job already holds every configured
// connection, a same-priority peer with no completion-critical work never
// earns a handout, so a refill on a busy pool is answered from the hot job's
// own queue.
async fn a_lane_refill_is_answered_from_the_hot_job_while_a_same_priority_peer_waits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        8,
    )
    .await;

    let hot_job_id = JobId(21440);
    let peer_job_id = JobId(21441);
    let hot_files = many_standalone_files("busy-hot", 600);
    let peer_files = many_standalone_files("busy-peer", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(standalone_job_spec("Busy Hot", &hot_files), "NORMAL"),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer_job_id,
        with_priority(standalone_job_spec("Busy Peer", &peer_files), "NORMAL"),
    )
    .await;

    pipeline.active_download_connections = 8;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 8);

    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));
    let peer_queued_before = pipeline
        .jobs
        .get(&peer_job_id)
        .unwrap()
        .download_queue
        .len();

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        lane_id: 0,
        runtime_generation: 0,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    let lease = response.lease.expect("the hot job still has queued work");
    assert_eq!(lease.job_id, hot_job_id);
    assert_eq!(
        pipeline
            .jobs
            .get(&peer_job_id)
            .unwrap()
            .download_queue
            .len(),
        peer_queued_before,
        "the peer's queue is untouched while the hot job can still be served"
    );
}

#[tokio::test]
async fn released_download_result_fences_completion_until_processed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20023);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));
    pipeline.note_released_download_result_pending(job_id, b"discarded".len() as u64);
    assert!(pipeline.job_has_pending_download_pipeline_work(job_id));
    assert_eq!(
        pipeline
            .pending_released_download_results_by_job
            .get(&job_id),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .pending_released_download_result_bytes_by_job
            .get(&job_id),
        Some(&(b"discarded".len() as u64))
    );

    pipeline
        .process_released_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from_static(b"discarded"))),
            attempts: vec![],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![],
            release_connection_slot: false,
        })
        .await;

    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));
    assert!(
        !pipeline
            .pending_released_download_results_by_job
            .contains_key(&job_id)
    );
    assert!(
        !pipeline
            .pending_released_download_result_bytes_by_job
            .contains_key(&job_id)
    );
}

#[tokio::test]
async fn failed_job_retains_released_result_ledgers_until_terminal_processing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20033);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Failed Released Result", &[("failed.bin".to_string(), 16)]),
    )
    .await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let payload = Bytes::from_static(b"failed-result");
    pipeline.note_released_download_result_pending(job_id, payload.len() as u64);

    pipeline.fail_job(job_id, "expected failure".to_string());

    assert_eq!(
        pipeline
            .pending_released_download_results_by_job
            .get(&job_id),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .pending_released_download_result_bytes_by_job
            .get(&job_id),
        Some(&(payload.len() as u64))
    );

    pipeline
        .process_released_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(payload)),
            attempts: vec![],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![],
            release_connection_slot: false,
        })
        .await;

    assert!(
        !pipeline
            .pending_released_download_results_by_job
            .contains_key(&job_id)
    );
    assert!(
        !pipeline
            .pending_released_download_result_bytes_by_job
            .contains_key(&job_id)
    );
}

#[tokio::test]
async fn owned_download_lane_batch_event_releases_and_acks_results() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20024);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let decoded_payload = b"owned-decoded".to_vec();

    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id: 0,
            results: vec![DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Decoded(DecodeResult {
                    encoding: SegmentEncoding::Yenc,
                    segment_id,
                    raw_size: decoded_payload.len() as u64,
                    yenc_layout: YencLayoutAssertions {
                        file_size: decoded_payload.len() as u64,
                        part: Some(1),
                        total: Some(1),
                        begin: Some(1),
                        end: Some(decoded_payload.len() as u64),
                    },
                    crc_valid: true,
                    part_crc_verified: false,
                    part_crc: par2_rs::checksum::crc32(&decoded_payload),
                    truncation_suspected: false,
                    expected_file_crc: None,
                    data: DecodedChunk::from(decoded_payload.clone()),
                    yenc_name: "owned.bin".to_string(),
                    checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                    segments: Vec::new(),
                })),
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: vec![],
                release_connection_slot: false,
            }],
            unrequested_works: vec![],
            stats: weaver_nntp::blocking::BlockingLaneStats::default(),
            ack: Some(ack),
        },
        &mut pending,
    );

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_downloads_by_job.get(&job_id), None);
    assert_eq!(
        pipeline.active_downloads_by_file.get(&segment_id.file_id),
        None
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pipeline
            .pending_released_download_results_by_job
            .get(&job_id),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .pending_released_download_result_bytes_by_job
            .get(&job_id),
        Some(&(decoded_payload.len() as u64))
    );
    assert!(ack_rx.try_recv().is_ok());

    pipeline
        .process_released_download_done(pending.pop_front().unwrap())
        .await;
    assert!(
        !pipeline
            .pending_released_download_results_by_job
            .contains_key(&job_id)
    );
    assert!(
        !pipeline
            .pending_released_download_result_bytes_by_job
            .contains_key(&job_id)
    );
}

#[tokio::test]
async fn owned_download_lane_requeues_unrequested_tail_without_retry_result() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20025);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Owned Tail Requeue", "owned-tail.bin", &[1024, 1024]),
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let tail_work = state.download_queue.pop().unwrap();

    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);
    pipeline
        .reserve_bandwidth_for_dispatch(segment_id, 2048)
        .unwrap();

    let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id: 0,
            results: vec![],
            unrequested_works: vec![tail_work],
            stats: weaver_nntp::blocking::BlockingLaneStats::default(),
            ack: Some(ack),
        },
        &mut pending,
    );

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_downloads_by_job.get(&job_id), None);
    assert_eq!(
        pipeline.active_downloads_by_file.get(&segment_id.file_id),
        None
    );
    assert!(pending.is_empty());
    assert!(
        !pipeline
            .pending_released_download_results_by_job
            .contains_key(&job_id)
    );
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 2);
    assert!(ack_rx.try_recv().is_ok());
}
