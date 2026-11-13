// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The decode stage: its queue and backlog budgets, and where an article whose
// body did not decode is fetched from next.

use super::*;
use crate::pipeline::orchestrator::{
    compute_decode_backlog_budget_bytes, compute_write_backlog_budget_bytes,
};

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

// `weaver_pipeline_decode_task_duration_seconds` is absent until the decode
// path has timed something, then reports exactly one observation per decode
// task — the single clock read the task is allowed, taken once at its end
// whichever way the task exits.
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
    wait_until(|| {
        metrics
            .pipeline_histograms
            .snapshot()
            .decode_task_duration
            .is_some_and(|histogram| histogram.count == 2)
    })
    .await;
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

// The active-decode gauge is published on every decode start and finish, so
// it reads the kept sum instead of walking every job's count each time.
#[tokio::test]
async fn the_active_decode_gauge_reads_the_kept_sum_without_walking_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment = |job: u64, segment_number: u32| SegmentId {
        file_id: NzbFileId {
            job_id: JobId(job),
            file_index: 0,
        },
        segment_number,
    };

    let walks = pipeline.active_decodes_by_job.walks();
    for (job, segment_number) in [(20041, 0), (20041, 1), (20042, 0)] {
        pipeline.note_decode_started(segment(job, segment_number), 64);
    }
    assert_eq!(pipeline.metrics.active_decodes.load(Ordering::Relaxed), 3);
    pipeline.active_decodes_by_job.remove(&JobId(20041));
    pipeline.publish_active_stage_metrics();
    assert_eq!(pipeline.metrics.active_decodes.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline.active_decodes_by_job.walks(),
        walks,
        "publishing the gauge must not walk the per-job counts"
    );
    assert_eq!(pipeline.active_decodes_by_job.values().sum::<usize>(), 1);
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
    let (mut pipeline, _, _) = new_direct_pipeline_with_backlog(&temp_dir, 5632 * 1024, 4).await;
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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
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

    let done = pipeline
        .decode_done_rx
        .recv()
        .await
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

    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_all()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.global_paused);
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(
        pipeline.db.get_setting("global_paused").unwrap().as_deref(),
        Some("true")
    );

    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .resume_all()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.global_paused);
    pipeline.db.flush_write_queue().await.unwrap();
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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
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

    let done = pipeline
        .decode_done_rx
        .recv()
        .await
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

    let work = pipeline
        .retry_rx
        .recv()
        .await
        .expect("decode failure should schedule a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
}

// A decoded article does not build its job's report: the job is marked, and
// the snapshot tick publishes the report once. The job's last decode
// publishes at once.
#[tokio::test]
async fn a_decoded_article_leaves_its_jobs_report_to_the_tick_until_the_last() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    let files = vec![
        ("one.bin".to_string(), 64u32),
        ("two.bin".to_string(), 64u32),
    ];
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Report Per Tick", &files),
    )
    .await;

    for file_index in 0..2 {
        let segment_id = SegmentId {
            file_id: NzbFileId { job_id, file_index },
            segment_number: 0,
        };
        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
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
    }
    let first = pipeline.decode_done_rx.recv().await.expect("first decode");
    let last = pipeline.decode_done_rx.recv().await.expect("last decode");
    let published = |pipeline: &Pipeline| {
        pipeline.shared_state.queue.read(|inner| {
            inner
                .job(job_id)
                .map(|row| row.report.job.failed_bytes)
                .expect("the job's row")
        })
    };

    pipeline.handle_decode_done(first).await;
    assert!(pipeline.reports_due.contains(&job_id));
    // The figure moves; the row says what it said until the tick.
    let before = published(&pipeline);
    pipeline.jobs.get_mut(&job_id).unwrap().failed_bytes = before + 7;
    assert_eq!(published(&pipeline), before);
    pipeline.refresh_periodic_snapshot();
    assert!(pipeline.reports_due.is_empty());
    assert_eq!(published(&pipeline), before + 7);

    pipeline.jobs.get_mut(&job_id).unwrap().failed_bytes = before + 9;
    pipeline.handle_decode_done(last).await;
    assert!(pipeline.reports_due.is_empty());
    assert_eq!(published(&pipeline), before + 9);
}

// A result or retry that only moves a job's figures leaves its report to the
// tick; one that moves its stage publishes at once. A job with no report
// writes nothing.
#[tokio::test]
async fn a_report_soon_defers_figures_and_publishes_a_stage_at_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20032);
    let files = vec![("one.bin".to_string(), 64u32)];
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Report Soon", &files),
    )
    .await;
    pipeline.report_job(job_id);
    let published = |pipeline: &Pipeline| {
        pipeline.shared_state.queue.read(|inner| {
            inner
                .job(job_id)
                .map(|row| {
                    (
                        row.report.job.failed_bytes,
                        row.report.job.finalizing_download,
                    )
                })
                .expect("the job's row")
        })
    };
    let (before, _) = published(&pipeline);

    pipeline.jobs.get_mut(&job_id).unwrap().failed_bytes = before + 5;
    pipeline.report_job_soon(job_id);
    assert!(pipeline.reports_due.contains(&job_id));
    assert_eq!(published(&pipeline), (before, false));

    pipeline.jobs_finalizing_download.insert(job_id);
    pipeline.report_job_soon(job_id);
    assert!(pipeline.reports_due.is_empty());
    assert_eq!(published(&pipeline), (before + 5, true));

    let revision = pipeline.shared_state.queue.revision();
    pipeline.report_job(JobId(20033));
    assert_eq!(pipeline.shared_state.queue.revision(), revision);
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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Decode {
                raw_size: 19,
                error: "missing =ybegin header".to_string(),
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

    let work = pipeline
        .retry_rx
        .recv()
        .await
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
                truncation_suspected: false,
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
    assert!(!pipeline.unanchored_parked.contains_key(&segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    let work = pipeline
        .retry_rx
        .recv()
        .await
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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Decoded(DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size: 8,
                // No declared size of its own, so nothing can justify a range
                // that runs past every envelope the job knows.
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"data!"),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(b"data!".to_vec()),
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
    assert!(!pipeline.unanchored_parked.contains_key(&segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    let work = pipeline
        .retry_rx
        .recv()
        .await
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
async fn single_server_decode_failure_gives_the_article_up_for_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let job_id = JobId(20017);
    let spec = segmented_job_spec("Single Server Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    // The only server produced a bad body: there is nowhere left to retry,
    // so the article fails now instead of queueing work no lane may serve.
    pipeline.handle_decode_failure(segment_id, "crc mismatch", &[], Some(0));

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(64)
    );
    assert_eq!(pipeline.metrics.segments_retried.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
    assert!(
        !pipeline
            .pending_retries_by_segment
            .contains_key(&segment_id)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn decode_failure_moves_to_the_remaining_server_then_gives_up() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(20018);
    let spec = segmented_job_spec("Two Server Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    // Server 0 produced a bad body; server 1 is still untried, so the
    // article moves there.
    pipeline.handle_decode_failure(segment_id, "crc mismatch", &[], Some(0));
    let work = pipeline
        .retry_rx
        .recv()
        .await
        .expect("a second server should draw a retry")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );

    // Server 1 produced a bad body too: every server has been tried, so the
    // article fails well inside the retry budget.
    pipeline.handle_decode_failure(segment_id, "crc mismatch", &work.exclude_servers, Some(1));
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(64)
    );
    assert_eq!(pipeline.metrics.segments_retried.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
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
            guessed: false,
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
    let backlog_base_bytes = 48 * 1024 * 1024;

    let ssd_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let hdd_budget = compute_write_backlog_budget_bytes(&hdd_profile, backlog_base_bytes);

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
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let backlog_base_bytes = 144 * 1024 * 1024;

    let write_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let decode_budget =
        compute_decode_backlog_budget_bytes(&profile, backlog_base_bytes, write_budget);

    assert!(decode_budget > write_budget);
    // Memory-scaled: available/12 dominates the tier's base term on an 8 GiB
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
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 250.0,
            random_read_iops: 5_000.0,
            same_filesystem: true,
        },
    };
    let backlog_base_bytes = 24 * 1024 * 1024;

    let write_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let decode_budget =
        compute_decode_backlog_budget_bytes(&profile, backlog_base_bytes, write_budget);

    assert_eq!(decode_budget, 64 * 1024 * 1024);
}

#[test]
fn backlog_budgets_share_one_memory_ceiling() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let host = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 8,
            logical_cores: 8,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * GIB,
            available_bytes: 16 * GIB,
            cgroup_limit: None,
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let mut boxed = host.clone();
    boxed.memory.cgroup_limit = Some(GIB);
    let mut small_host = host.clone();
    small_host.memory.available_bytes = GIB;
    // A 384 MiB base: above an eighth of the container's limit, well
    // below an eighth of the host's memory.
    let backlog_base_bytes = 384 * 1024 * 1024;
    let budgets = |profile: &SystemProfile| {
        let write = compute_write_backlog_budget_bytes(profile, backlog_base_bytes);
        let decode = compute_decode_backlog_budget_bytes(profile, backlog_base_bytes, write);
        (write, decode)
    };

    // Without a container limit the host figure decides, as before.
    assert_eq!(
        budgets(&host),
        (384 * 1024 * 1024, (16 * GIB / 12) as usize)
    );
    // A container limit scales both budgets down from the same ceiling the
    // host would have with that much memory.
    assert_eq!(budgets(&boxed), ((GIB / 8) as usize, (GIB / 8) as usize));
    assert_eq!(budgets(&boxed), budgets(&small_host));
}

// The backlog limits and the pending writes per file come out of the memory
// tier exactly as they did when the tier sized an article buffer pool and the
// limits were read off that pool's size. The old arithmetic is copied here as
// the reference, so any drift in the tier table or the limit formulas fails.
#[test]
fn backlog_sizing_matches_the_former_pool_sized_limits_on_every_host() {
    use crate::runtime::backlog_sizing::BacklogSizing;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    // The former pool: buffer counts per tier by memory.
    fn old_counts(available_bytes: u64, cgroup_limit: Option<u64>) -> (usize, usize, usize) {
        const UNCAPPED_HOST_MEMORY_CAP_BYTES: u64 = (8 * 1024 * 1024 * 1024) - 1;
        let sizing =
            cgroup_limit.unwrap_or_else(|| available_bytes.min(UNCAPPED_HOST_MEMORY_CAP_BYTES));
        let available_mb = (sizing / (1024 * 1024)) as usize;
        if available_mb < 1024 {
            (32, 4, 1)
        } else if available_mb < 2048 {
            (64, 8, 2)
        } else if available_mb < 4096 {
            (128, 16, 4)
        } else if available_mb < 8192 {
            (192, 24, 6)
        } else if available_mb < 16384 {
            (256, 32, 8)
        } else if available_mb < 32768 {
            (384, 48, 12)
        } else {
            (512, 64, 16)
        }
    }
    fn old_pool_total_bytes((small, medium, large): (usize, usize, usize)) -> usize {
        small * 512 * 1024 + medium * 1024 * 1024 + large * 4 * 1024 * 1024
    }
    fn old_write_max_pending((small, _, _): (usize, usize, usize)) -> usize {
        if small <= 64 {
            4
        } else if small <= 128 {
            8
        } else {
            16
        }
    }
    fn old_ceiling(profile: &SystemProfile) -> usize {
        let available_bytes = profile.memory.available_bytes.max(256 * 1024 * 1024);
        match profile.memory.cgroup_limit {
            Some(cgroup_limit) => available_bytes.min(cgroup_limit) as usize,
            None => available_bytes as usize,
        }
    }
    fn old_write_budget(profile: &SystemProfile, scratch_bytes: usize) -> usize {
        let available_bytes = old_ceiling(profile);
        let base = scratch_bytes
            .max(64 * 1024 * 1024)
            .min((available_bytes / 8).max(64 * 1024 * 1024));
        match profile.disk.storage_class {
            StorageClass::Ssd => base,
            StorageClass::Hdd => (base.saturating_mul(2)).min((available_bytes / 4).max(base)),
            StorageClass::Network | StorageClass::Unknown => {
                (base.saturating_mul(3) / 2).min((available_bytes / 5).max(base))
            }
        }
    }
    fn old_decode_budget(profile: &SystemProfile, scratch_bytes: usize, write: usize) -> usize {
        let available_bytes = old_ceiling(profile);
        let scaled = available_bytes / 12;
        let memory_cap = (available_bytes / 8).max(write).min(4 * 1024 * 1024 * 1024);
        let target = scratch_bytes.saturating_mul(3).max(scaled).max(write);
        target.min(memory_cap).max(1)
    }

    let mut checked = 0;
    for available in [
        512 * MIB,
        GIB,
        2 * GIB,
        4 * GIB,
        8 * GIB,
        16 * GIB,
        32 * GIB,
        64 * GIB,
    ] {
        for cgroup_limit in [
            None,
            Some(GIB),
            Some(4 * GIB),
            Some(16 * GIB),
            Some(64 * GIB),
        ] {
            for storage_class in [StorageClass::Ssd, StorageClass::Hdd, StorageClass::Network] {
                let profile = SystemProfile {
                    cpu: CpuProfile {
                        physical_cores: 4,
                        logical_cores: 4,
                        simd: SimdSupport::default(),
                        cgroup_limit: None,
                    },
                    memory: MemoryProfile {
                        total_bytes: available,
                        available_bytes: available,
                        cgroup_limit,
                        guessed: false,
                    },
                    disk: DiskProfile {
                        storage_class: storage_class.clone(),
                        filesystem: FilesystemType::Ext4,
                        sequential_write_mbps: 500.0,
                        random_read_iops: 10_000.0,
                        same_filesystem: true,
                    },
                };
                let case = format!("{available} bytes, limit {cgroup_limit:?}, {storage_class:?}");

                let counts = old_counts(available, cgroup_limit);
                let old_scratch = old_pool_total_bytes(counts);
                let old_write = old_write_budget(&profile, old_scratch);
                let old_decode = old_decode_budget(&profile, old_scratch, old_write);

                let sizing = BacklogSizing::for_runtime_memory(available, cgroup_limit);
                let write = compute_write_backlog_budget_bytes(&profile, sizing.base_bytes);
                let decode =
                    compute_decode_backlog_budget_bytes(&profile, sizing.base_bytes, write);

                assert_eq!(sizing.base_bytes, old_scratch, "{case}");
                assert_eq!(write, old_write, "write backlog limit, {case}");
                assert_eq!(decode, old_decode, "decode backlog limit, {case}");
                assert_eq!(
                    sizing.pending_writes_per_file,
                    old_write_max_pending(counts),
                    "pending writes per file, {case}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 8 * 5 * 3);
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
    // A decoded part held for the part before it, the write backlog's charge
    // for it booked as the park books it.
    let data = vec![3u8; 4096];
    let parked = DecodeResult {
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 1,
        },
        raw_size: 4096,
        yenc_layout: YencLayoutAssertions {
            file_size: 0,
            part: None,
            total: None,
            begin: None,
            end: None,
        },
        crc_valid: true,
        part_crc_verified: true,
        part_crc: par2_rs::checksum::crc32(&data),
        truncation_suspected: false,
        expected_file_crc: None,
        segments: Vec::new(),
        data: DecodedChunk::from(Vec::<u8>::new()),
        yenc_name: "stalled.bin".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
    };
    let parked_len = data.len();
    pipeline.unanchored_parked.insert(
        file_id,
        1,
        crate::pipeline::UnanchoredPart {
            result: parked,
            bytes: crate::pipeline::ParkedBytes::Memory(DecodedChunk::from(data)),
            source: SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        },
    );
    pipeline.note_write_buffered(parked_len, 1);
    assert_eq!(pipeline.unanchored_parked.parts(), 1);

    pipeline.fail_job(job_id, "forced failure".to_string());

    settle_failed_releases(&mut pipeline).await;

    assert!(!pipeline.unanchored_parked.contains_key(&file_id));
    assert_eq!(pipeline.unanchored_parked.parts(), 0);
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
                truncation_suspected: false,
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
    assert!(
        matches!(
            &status,
            JobStatus::Failed { error }
                if error.contains("write path failed") && error.contains("blocked.bin")
        ),
        "{status:?}"
    );
    settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(working_dir.join("blocked.bin").is_dir());
}
