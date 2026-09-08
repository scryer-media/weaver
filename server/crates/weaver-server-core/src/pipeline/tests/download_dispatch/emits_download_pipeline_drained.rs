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
            .is_some_and(|ready_at| *ready_at > Instant::now())
    );
    let retry_delay = pipeline
        .next_restart_durable_lead_retry_delay()
        .expect("durable lead block should expose a retry wakeup");
    assert!(retry_delay <= Duration::from_millis(250));

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
        .insert(job_id, Instant::now() + Duration::from_millis(250));

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
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 99,
        },
        decoded_size: 4096,
        data: DecodedChunk::from(vec![7u8; 4096]),
        part_crc: par2_rs::checksum::crc32(&vec![7u8; 4096]),
        part_crc_verified: true,
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

    let pressure = pipeline.refresh_download_pressure();
    let lease = pipeline
        .try_lease_initial_download_batch_for_test(job_id, pressure)
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

    assert_eq!(pipeline.active_downloads, 1);
    let first_job_id = cases[0].0;
    let first_state = pipeline.jobs.get(&first_job_id).unwrap();
    assert_eq!(&first_state.status, &cases[0].1);
    assert_eq!(first_state.download_queue.len(), 0);
    assert!(pipeline.active_download_passes.contains(&first_job_id));

    pipeline.active_downloads = 0;
    pipeline.active_download_connections = 0;
    pipeline.active_downloads_by_job.clear();
    pipeline.active_download_connections_by_job.clear();
    pipeline.active_downloads_by_file.clear();

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    let second_job_id = cases[1].0;
    let second_state = pipeline.jobs.get(&second_job_id).unwrap();
    assert_eq!(&second_state.status, &cases[1].1);
    assert_eq!(second_state.download_queue.len(), 0);
    assert!(pipeline.active_download_passes.contains(&second_job_id));
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
    assert_eq!(pipeline.hot_dispatch_job, Some(normal_job_id));
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

    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline.hot_dispatch_job,
        Some(promoted_job_id),
        "the High update must replace the retained hot selection"
    );
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
// Proactive same-priority sharing (the old bounded-same-band arm) is gone:
// a healthy hot job with enough queued work to fill capacity takes every
// connection, even with same-priority peers waiting. Spillover only ever
// engages once the hot job is measurably slow — see
// `dispatch_downloads_shares_slots_after_hot_job_underfills` for that path.
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
    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
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
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0
    );
}

#[tokio::test]
async fn completion_critical_recovery_takes_every_lane_ahead_of_higher_priority_primary() {
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

    // The critical-first phase has no lane cap: it drains ahead of every
    // regular byte on every job, including the higher-priority hot job's own
    // queue, until capacity or critical demand runs out — whichever first.
    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&critical_job_id),
        Some(&8)
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&hot_job_id),
        "critical demand saturates capacity before the hot job's regular queue is touched"
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0,
        "completion-critical work is dispatched directly, never as a spillover loan"
    );
}

#[tokio::test]
async fn completion_critical_recovery_wins_the_only_connection() {
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
        pipeline
            .active_download_connections_by_job
            .get(&critical_job_id),
        Some(&1)
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&hot_job_id)
    );
}

// completion_critical_share_caps_mixed_hot_job_by_lane_class removed: it
// pinned the old max/4-clamped bounded-completion-critical-share cap (a hot
// job's own critical work was throttled to 2 of 8 lanes even though it owned
// the connections outright). That cap no longer exists — completion-critical
// work has no lane limit now, on the hot job's own queue or anyone else's.
// See `completion_critical_recovery_takes_every_lane_ahead_of_higher_priority_primary`
// and `critical_work_drains_before_regular_bytes_across_all_jobs` for the
// uncapped, cross-job replacement coverage.

// dispatch_downloads_bounded_same_band_skips_blocked_peer_and_tries_next
// removed: it pinned the old proactive same-band share's skip-blocked-peer
// search among same-priority peers while the hot job was healthy. There is
// no such search anymore — a healthy hot job with queued work takes every
// lane (see `dispatch_downloads_hot_job_takes_full_capacity_over_same_priority_peers`),
// and the only remaining peer-selection logic is the measured-underfill
// spillover picker exercised by `dispatch_downloads_shares_slots_after_hot_job_underfills`.

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

    assert_eq!(pipeline.hot_dispatch_job, Some(job_id));
    assert_eq!(pipeline.active_download_connections, 8);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&job_id),
        Some(&8)
    );
    // First dispatch wave has no measured throughput yet, so lanes lease the
    // cold-start batch rather than the full hot runway.
    assert_eq!(
        pipeline.active_downloads_by_job.get(&job_id),
        Some(&(8 * TEST_HOT_LEASE_COLD_START_WORK_LIMIT))
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0
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

    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&8)
    );
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&low_job_id),
        None
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0
    );
}

#[tokio::test]
async fn dispatch_downloads_pressure_disables_bounded_share() {
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

    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
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
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0
    );
}

#[tokio::test]
async fn hot_share_yield_signal_clears_when_dispatch_gates_disable_bounded_share() {
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

    pipeline.hot_share_yield_signal.request();
    pipeline.global_paused = true;
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested());
    pipeline.global_paused = false;

    pipeline.hot_share_yield_signal.request();
    pipeline.rate_limiter.set_rate(1_000);
    pipeline.rate_limiter.refund(1_000);
    pipeline.rate_limiter.consume(1_500);
    assert!(pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested());

    pipeline.hot_share_yield_signal.request();
    let now = chrono::Local::now();
    let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
    pipeline
        .db
        .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 1024)
        .unwrap();
    pipeline
        .apply_bandwidth_cap_policy(Some(crate::bandwidth::IspBandwidthCapConfig {
            enabled: true,
            period: crate::bandwidth::IspBandwidthCapPeriod::Daily,
            limit_bytes: 512,
            reset_time_minutes_local: reset_minutes,
            weekly_reset_weekday: crate::bandwidth::IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }))
        .unwrap();
    pipeline.rate_limiter.refund(2_000);
    assert!(!pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested());

    pipeline.apply_bandwidth_cap_policy(None).unwrap();
    pipeline.hot_share_yield_signal.request();
    pipeline.decode_backlog_budget_bytes = 1_000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(1_000, Ordering::Relaxed);
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested());

    pipeline.hot_share_yield_signal.request();
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);
    pipeline.download_pressure_soft_dispatch_after = Some(Instant::now() + Duration::from_secs(5));
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested());
}

#[tokio::test]
async fn hot_share_yield_signal_clears_when_refill_gates_disable_bounded_share() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21433);

    pipeline.hot_share_yield_signal.request();
    pipeline.global_paused = true;
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: FileRole::Standalone.download_priority(),
            is_recovery: false,
            completion_critical: false,
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            exclude_servers: Vec::new(),
            avoid_server: None,
        },
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert!(!pipeline.hot_share_yield_signal.is_requested());
}

#[tokio::test]
async fn dispatch_downloads_shares_slots_after_hot_job_underfills() {
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

    let earlier_job_id = JobId(20019);
    let later_job_id = JobId(20020);

    insert_active_job(
        &mut pipeline,
        earlier_job_id,
        with_priority(
            standalone_job_spec(
                "Earlier Normal Fair Share",
                &[("earlier.bin".to_string(), 512u32)],
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        later_job_id,
        with_priority(
            standalone_job_spec(
                "Later More Progress Fair Share",
                &[
                    ("later-0.bin".to_string(), 512u32),
                    ("later-1.bin".to_string(), 512u32),
                    ("later-2.bin".to_string(), 512u32),
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
    assert_eq!(pipeline.active_download_connections, 1);
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
        2
    );
    assert_eq!(
        pipeline.active_downloads_by_job.get(&earlier_job_id),
        Some(&1)
    );
    assert_eq!(pipeline.active_downloads_by_job.get(&later_job_id), None);
    assert_eq!(pipeline.hot_dispatch_job, Some(earlier_job_id));
    // Unused capacity exists (the hot job's own queue just drained), but the
    // short slowness-detection window has not elapsed yet, so spillover does
    // not engage on the very first pass. There is no warmup gate anymore —
    // this window is the only reason a first pass ever holds capacity back.
    assert!(pipeline.hot_dispatch_underfill_since.is_some());
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Exclusive);
    assert!(
        pipeline
            .metrics
            .hot_dispatch_spillover_blocked_hot_can_use_capacity_total
            .load(Ordering::Relaxed)
            >= 1
    );

    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(2));

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 2);
    assert_eq!(pipeline.active_download_connections, 2);
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
    assert_eq!(
        pipeline.active_downloads_by_job.get(&earlier_job_id),
        Some(&1)
    );
    assert_eq!(
        pipeline.active_downloads_by_job.get(&later_job_id),
        Some(&1)
    );
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Shared);
    assert!(
        pipeline
            .metrics
            .hot_dispatch_spillover_allowed_measured_underfill_total
            .load(Ordering::Relaxed)
            >= 1
    );
    assert_eq!(pipeline.hot_dispatch_spillover_loans.active_loan_count(), 1);
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        1
    );

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Shared);
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        1
    );
}

/// New-contract test: completion-critical work across three jobs drains
/// before any regular byte, on every job including the hot one, with no
/// lane cap — the exact replacement for the old bounded max/4 share.
#[tokio::test]
async fn critical_work_drains_before_regular_bytes_across_all_jobs() {
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
                // More than one cold-start lease's worth (16 articles), so
                // the hot fill loop needs two connections to use the two
                // lanes the critical phase left it, not just one.
                &many_standalone_files("critical-first-hot", 20),
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

    // Every connection critical demand could use went to the two critical
    // jobs, one lane each (their single item apiece), before the hot job's
    // ten-file regular queue saw any of the remaining capacity.
    assert_eq!(pipeline.active_completion_critical_connections, 2);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&critical_a),
        Some(&1)
    );
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&critical_b),
        Some(&1)
    );
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&2)
    );
    assert_eq!(pipeline.active_download_connections, 4);
    assert!(
        pipeline
            .jobs
            .get(&critical_a)
            .unwrap()
            .download_queue
            .is_empty()
    );
    assert!(
        pipeline
            .jobs
            .get(&critical_b)
            .unwrap()
            .download_queue
            .is_empty()
    );
}

/// Critical work that is queued but UNDISPATCHABLE must not latch the yield
/// signal: a propagation-held job's critical demand cannot be served by any
/// yielded lane, so asking every regular lane to park behind it would wedge
/// the whole queue on work nothing can act on. The critical phase reports
/// such demand as drained, and the signal clears.
#[tokio::test]
async fn undispatchable_critical_work_does_not_ask_regular_lanes_to_yield() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 4,
            medium_count: 2,
            large_count: 1,
        },
        2,
    )
    .await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));

    let regular_job_id = JobId(21520);
    let held_job_id = JobId(21521);
    insert_active_job(
        &mut pipeline,
        regular_job_id,
        standalone_job_spec(
            "Yield Regular Runner",
            // More than one cold-start lease's worth (16 articles), so both
            // connections are needed to serve the queue.
            &many_standalone_files("yield-regular-runner", 20),
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        held_job_id,
        posted_job_spec("Yield Held Fresh Post", Some(now_epoch_secs())),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&held_job_id).unwrap();
        for mut work in state.download_queue.drain_all() {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
    }
    assert!(
        pipeline.propagation_hold_until(held_job_id).is_some(),
        "non-vacuity: the critical job must actually be held, or this test \
         exercises the ordinary starved path instead"
    );

    pipeline.dispatch_downloads();

    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&regular_job_id),
        Some(&2),
        "the regular job takes the capacity the held critical job cannot use"
    );
    assert!(
        !pipeline.hot_share_yield_signal.is_requested(),
        "held critical demand is drained, not starved: no lane may be asked \
         to yield for work no freed connection could be handed to"
    );
}

/// The counterpart: critical demand that capacity (not dispatchability) is
/// holding back keeps the yield request alive so mid-batch lanes return
/// their tails for the next pass's critical-first phase.
#[tokio::test]
async fn capacity_starved_critical_demand_keeps_the_yield_request() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 4,
            medium_count: 2,
            large_count: 1,
        },
        2,
    )
    .await;

    let regular_job_id = JobId(21530);
    let critical_job_id = JobId(21531);
    insert_active_job(
        &mut pipeline,
        regular_job_id,
        standalone_job_spec(
            "Starved Regular Runner",
            // More than one cold-start lease's worth (16 articles), so both
            // connections fill with regular bytes.
            &many_standalone_files("starved-regular-runner", 20),
        ),
    )
    .await;
    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline.active_download_connections, 2,
        "non-vacuity: capacity must be full of regular bytes before the \
         critical work arrives"
    );

    insert_active_job(
        &mut pipeline,
        critical_job_id,
        standalone_job_spec(
            "Starved Critical Arrival",
            &[("starved-critical.bin".to_string(), 512u32)],
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&critical_job_id).unwrap();
        for mut work in state.download_queue.drain_all() {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
    }

    pipeline.dispatch_downloads();

    assert!(
        pipeline.hot_share_yield_signal.is_requested(),
        "dispatchable critical demand blocked only by full capacity must ask \
         the busy lanes to yield"
    );
}

/// New-contract test: a hot job with queued primary work is never spilled
/// away from, no matter how long unused capacity has technically existed —
/// `HotHasQueuedPrimary`/`LaneCapacityAvailable` block spillover before the
/// slowness window is ever consulted.
#[tokio::test]
async fn hot_job_with_queued_primary_never_spills_regardless_of_underfill_duration() {
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

    let hot_job_id = JobId(21510);
    let peer_job_id = JobId(21511);
    // More than two cold-start leases' worth (16 articles each), so the hot
    // fill loop uses both of the two configured connections and still has
    // files left queued afterward.
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Never Spill Hot",
                &many_standalone_files("never-spill-hot", 40),
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer_job_id,
        with_priority(
            standalone_job_spec(
                "Never Spill Peer",
                &many_standalone_files("never-spill-peer", 5),
            ),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&2)
    );
    // The hot job's own queue still has unleased files: capacity is fully
    // committed to it, not "unused", so there is nothing to spill even before
    // the window comes into play.
    assert!(
        !pipeline
            .jobs
            .get(&hot_job_id)
            .unwrap()
            .download_queue
            .is_empty()
    );

    // Backdate the slowness window as far as it will go and dispatch again —
    // it must not matter, because the hot job still has queued primary work.
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(3600));
    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&peer_job_id),
        None
    );
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Exclusive);
    assert_eq!(
        pipeline.hot_dispatch_last_spillover_decision,
        SpilloverDecision::BlockedHotCanUseCapacity
    );
}

/// New-contract test: once the hot job is genuinely idle, spillover engages,
/// but at most two distinct non-hot jobs may hold a loan at a time — a third
/// job with real queued work and free capacity is still refused.
#[tokio::test]
async fn spillover_caps_distinct_jobs_at_two_even_with_capacity_to_spare() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        6,
    )
    .await;

    let hot_job_id = JobId(21520);
    // peer_a and peer_b are never registered as real jobs: they exist only
    // in the loan book (pre-seeded as already holding a loan) and in the
    // per-job connection counters, which is all the two-job cap needs to
    // see. peer_c is a real job with real queued work, so it is the one
    // candidate the dispatch loop actually considers admitting.
    let peer_a = JobId(21521);
    let peer_b = JobId(21522);
    let peer_c = JobId(21523);

    // The hot job has nothing left queued, but one connection still active
    // (a body in flight), so it keeps hot status without qualifying for the
    // idle fast path — spillover here grows at the normal, budget-limited
    // pace, which does not matter to the cap this test is pinning.
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Cap Two Hot", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&hot_job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }
    insert_active_job(
        &mut pipeline,
        peer_c,
        with_priority(
            standalone_job_spec(
                "Cap Two Peer C",
                &many_standalone_files("cap-two-peer-c", 20),
            ),
            "NORMAL",
        ),
    )
    .await;

    let now = Instant::now();
    pipeline.hot_dispatch_job = Some(hot_job_id);
    pipeline.hot_dispatch_started_at = Some(now - Duration::from_secs(5));
    pipeline.hot_dispatch_mode = DispatchShareMode::Shared;
    pipeline.hot_dispatch_underfill_since = Some(now - Duration::from_secs(2));
    pipeline.active_download_connections = 3;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(peer_a, 1);
    pipeline
        .active_download_connections_by_job
        .insert(peer_b, 1);
    pipeline.hot_dispatch_spillover_loans.start_or_extend(
        peer_a,
        now,
        10_000,
        SpilloverLoanKind::MeasuredUnderfill,
    );
    pipeline.hot_dispatch_spillover_loans.start_or_extend(
        peer_b,
        now,
        10_000,
        SpilloverLoanKind::MeasuredUnderfill,
    );
    assert_eq!(
        pipeline.hot_dispatch_spillover_loans.distinct_loan_jobs(),
        2
    );

    pipeline.dispatch_downloads();

    // peer_c never receives a connection, even though it has 20 real files
    // queued and three of the six configured connections sat unused,
    // because admitting it would make a third distinct spilled-to job.
    assert_eq!(pipeline.active_download_connections, 3);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&peer_c),
        None
    );
    assert_eq!(
        pipeline.hot_dispatch_spillover_loans.distinct_loan_jobs(),
        2
    );
    assert!(!pipeline.hot_dispatch_spillover_loans.holds_loan(peer_c));
}

/// New-contract test: a spillover loan is reclaimed as soon as the hot job
/// regains queued primary work — refill for the loaned lane is denied even
/// though nothing measured a speed harm.
#[tokio::test]
async fn lane_refill_reclaims_spillover_when_hot_regains_queued_work() {
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

    let hot_job_id = JobId(21530);
    let spillover_job_id = JobId(21531);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Regain Hot", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    {
        // The hot job's initial file is already "in flight" for the purposes
        // of this test — clear the queue so it starts genuinely empty, the
        // state a real loan would have been granted against.
        let state = pipeline.jobs.get_mut(&hot_job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }
    insert_active_job(
        &mut pipeline,
        spillover_job_id,
        with_priority(
            standalone_job_spec(
                "Regain Spillover",
                &many_standalone_files("regain-spillover", 5),
            ),
            "NORMAL",
        ),
    )
    .await;

    let now = Instant::now();
    pipeline.hot_dispatch_job = Some(hot_job_id);
    pipeline.hot_dispatch_started_at = Some(now - Duration::from_secs(5));
    pipeline.hot_dispatch_mode = DispatchShareMode::Shared;
    pipeline.active_download_connections = 2;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(spillover_job_id, 1);
    pipeline.hot_dispatch_spillover_loans.start_or_extend(
        spillover_job_id,
        now,
        10_000,
        SpilloverLoanKind::MeasuredUnderfill,
    );

    // The hot job's queue was empty when the loan started; it now has new
    // primary work queued (e.g. a later NZB segment arrived), so the loaned
    // lane must yield back on its next refill instead of continuing.
    assert!(
        pipeline
            .jobs
            .get(&hot_job_id)
            .unwrap()
            .download_queue
            .is_empty()
    );
    {
        let state = pipeline.jobs.get_mut(&hot_job_id).unwrap();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: hot_job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            message_id: MessageId::new("regain-hot-1@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: FileRole::Standalone.download_priority(),
            byte_estimate: 512,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: vec![],
            avoid_server: None,
        });
    }

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&spillover_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: spillover_job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: Some(SpilloverLoanKind::MeasuredUnderfill),
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::SpilloverWithdraw);
    // Reclaiming out of an already-`Shared` period records the general
    // `Reclaimed` decision (mirroring the speed-harm reclaim path); the
    // specific `BlockedHotCanUseCapacity` reason still drives it, visible in
    // the best-mode-block-reason gauge the refill call also set.
    assert_eq!(
        pipeline.hot_dispatch_last_spillover_decision,
        SpilloverDecision::Reclaimed
    );
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Exclusive);
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
        // 8 connections x a 16-article cold-start lease each: comfortably
        // more than that so the pass is capacity-bound, not work-bound.
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

    assert_eq!(pipeline.hot_dispatch_job, Some(job_id));
    assert_eq!(pipeline.active_download_connections, 1);
    // First dispatch has no measured throughput yet, so the hot lane leases
    // the cold-start batch; the full 64-article runway resumes once the
    // throughput window has data (see hot_lease_work_limit_scales_with_lane_throughput).
    assert_eq!(pipeline.active_downloads, 16);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 84);
    assert_eq!(
        pipeline
            .metrics
            .download_lane_lease_items_total
            .load(Ordering::Relaxed),
        16
    );
}

#[tokio::test]
// Hot fills everything: even holding every configured connection itself, a
// same-priority peer with no completion-critical work never earns a share,
// so the hot job's own lane refill is granted rather than yielded. This is
// the direct replacement for the old bounded-same-band yield contract.
async fn hot_lane_refill_is_granted_when_hot_holds_all_capacity_and_peer_has_no_critical_work() {
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
    let hot_files = many_standalone_files("yield-hot", 600);
    let peer_files = many_standalone_files("yield-peer", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(standalone_job_spec("Yield Hot", &hot_files), "NORMAL"),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer_job_id,
        with_priority(standalone_job_spec("Yield Peer", &peer_files), "NORMAL"),
    )
    .await;

    pipeline.hot_dispatch_job = Some(hot_job_id);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(5));
    pipeline.active_download_connections = 8;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 8);
    pipeline.active_downloads_by_job.insert(
        hot_job_id,
        8 * TEST_HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT,
    );

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&hot_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: hot_job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_some());
    assert_eq!(response.park_reason, LaneParkReason::NoWork);
    assert!(!pipeline.hot_share_yield_signal.is_requested());
}

#[tokio::test]
async fn hot_lane_refill_yields_to_higher_priority_completion_critical_work_without_hot_thrashing()
{
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

    let hot_job_id = JobId(21442);
    let critical_job_id = JobId(21443);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Critical Yield Hot",
                &many_standalone_files("critical-yield-hot", 600),
            ),
            "LOW",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        critical_job_id,
        with_priority(
            standalone_job_spec(
                "Critical Yield Peer",
                &many_standalone_files("critical-yield-peer", 600),
            ),
            "HIGH",
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&critical_job_id).unwrap();
        for mut work in state.download_queue.drain_all() {
            work.is_recovery = true;
            work.completion_critical = true;
            state.download_queue.push(work);
        }
    }

    pipeline.hot_dispatch_job = Some(hot_job_id);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(5));
    pipeline.active_download_connections = 8;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 8);
    pipeline.active_downloads_by_job.insert(
        hot_job_id,
        8 * TEST_HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT,
    );

    // The dispatch pass owns the yield signal now: with capacity full of the
    // hot job's regular bytes, the critical-first phase reports capacity
    // starvation and raises it. Refill only reads the signal, so the pass
    // must run first — exactly the production order.
    pipeline.dispatch_downloads();
    assert!(
        pipeline.hot_share_yield_signal.is_requested(),
        "non-vacuity: capacity-starved critical demand must have raised the \
         yield signal before the refill request arrives"
    );

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&hot_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: hot_job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::HotShareYield);
    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert!(pipeline.hot_share_yield_signal.is_requested());
}

// bounded_same_band_refill_survives_hot_queued_primary removed:
// `SpilloverLoanKind::BoundedSameBand` no longer exists, and its defining
// property — a shared lane surviving refill even though the hot job has
// queued primary work — is exactly the proactive-sharing behavior the new
// model eliminates. A `MeasuredUnderfill` loan in the same setup is instead
// expected to be *reclaimed* once the hot job has queued primary work and
// capacity to use it (`LaneCapacityAvailable` blocks `can_keep_lent_lane`);
// see `hot_lane_refill_yields_when_bounded_share_unmet`'s replacement and the
// new spill-reclaims-on-hot-regaining-work contract test for that path.

#[tokio::test]
async fn lane_refill_preserves_same_band_spillover_after_underfill() {
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

    let hot_job_id = JobId(20025);
    let spillover_job_id = JobId(20026);
    let spillover_files = (0..17)
        .map(|idx| (format!("spillover-{idx}.bin"), 512u32))
        .collect::<Vec<_>>();

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Hot Refill Normal", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        spillover_job_id,
        with_priority(
            standalone_job_spec("Spillover Refill Normal", &spillover_files),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(5));
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(2));

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Shared);
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline
            .jobs
            .get(&spillover_job_id)
            .unwrap()
            .download_queue
            .len(),
        16
    );

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&spillover_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: spillover_job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: Some(SpilloverLoanKind::MeasuredUnderfill),
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    let lease = response.lease.expect("shared spillover lane should refill");
    assert_eq!(lease.job_id, spillover_job_id);
    assert_eq!(lease.works.len(), 1);
    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Shared);
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline
            .jobs
            .get(&spillover_job_id)
            .unwrap()
            .download_queue
            .len(),
        15
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_granted_total
            .load(Ordering::Relaxed),
        1
    );
}

// dispatch_downloads_blocks_spillover_when_recent_expansion_helped removed:
// the `RecentExpansionHelped` spillover-block gate is gone. The expansion
// window itself (`hot_dispatch_expansion_window`) stays — it still feeds the
// informational `hot_dispatch_recent_expansion_improvement_pct` /
// `hot_dispatch_last_expansion_*` metrics — but a recent expansion no longer
// withholds spillover from a job that is otherwise measurably slow; only
// `HotHasQueuedPrimary` and `LaneCapacityAvailable` do that now (see
// `hot_best_mode_block_reason`).

#[tokio::test]
async fn lane_refill_reclaims_spillover_after_measured_speed_harm() {
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

    let hot_job_id = JobId(20029);
    let spillover_job_id = JobId(20030);
    let spillover_files = (0..17)
        .map(|idx| (format!("speed-harm-spillover-{idx}.bin"), 512u32))
        .collect::<Vec<_>>();

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Hot Speed Harm", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        spillover_job_id,
        with_priority(
            standalone_job_spec("Spillover Speed Harm", &spillover_files),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(5));
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(2));
    pipeline.dispatch_downloads();

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&spillover_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        1
    );
    pipeline.hot_dispatch_spillover_loans.mark_reclaim_for_test(
        spillover_job_id,
        Some(9_000),
        SpilloverReclaimReason::SpeedHarm,
    );

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: spillover_job_id,
        server_idx: 0,
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: Some(SpilloverLoanKind::MeasuredUnderfill),
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::SpilloverSpeedHarm);
    assert!(
        pipeline
            .hot_dispatch_spillover_loans
            .reclaim_pending_for(spillover_job_id)
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        1
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        1
    );

    pipeline.handle_download_lane_parked(DownloadLaneParked {
        job_id: spillover_job_id,
        mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: Some(SpilloverLoanKind::MeasuredUnderfill),
        completion_critical: false,
        reason: LaneParkReason::SpilloverSpeedHarm,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });
    assert!(
        !pipeline
            .hot_dispatch_spillover_loans
            .reclaim_pending_for(spillover_job_id)
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .active_lent_connections(),
        0
    );
}

// release_download_result_updates_hot_job_successes_before_heavy_processing
// removed: it pinned the `hot_dispatch_successes` counter and the warmup
// gate it fed (`hot_dispatch_warmup_complete`), both deleted — the pipeline
// hits full configured capacity from the first dispatch pass now, so there
// is nothing left to count successes toward.

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
            results: vec![DownloadResult {
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
