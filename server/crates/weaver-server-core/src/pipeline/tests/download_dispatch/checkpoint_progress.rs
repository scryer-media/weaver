use super::*;

#[tokio::test]
async fn checkpoint_deadline_is_consumed_when_capacity_pressure_or_pause_bypasses_selection() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    insert_active_job(
        &mut pipeline,
        PEER,
        segmented_job_spec("Ready peer", "peer.bin", &[100; 640]),
    )
    .await;
    pipeline.dispatch_downloads();
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(30));
    for _ in 0..4 {
        pipeline.dispatch_downloads();
    }
    assert_eq!(pipeline.active_download_connections, 4);
    assert_eq!(pipeline.active_download_connections_by_job[&PEER], 3);
    for mode in 0..3 {
        pipeline.global_paused = mode == 1;
        pipeline
            .metrics
            .write_buffered_bytes
            .store(if mode == 2 { u64::MAX } else { 0 }, Ordering::Relaxed);
        pipeline
            .download_restart_durable_lead_retry_after
            .insert(HOT, Instant::now() - Duration::from_secs(1));
        assert_eq!(
            pipeline.next_restart_durable_lead_retry_delay(),
            Some(Duration::ZERO)
        );
        pipeline.consume_due_checkpoint_rechecks();
        for _ in 0..10 {
            pipeline.dispatch_downloads();
            assert!(
                pipeline
                    .next_restart_durable_lead_retry_delay()
                    .is_none_or(|delay| !delay.is_zero())
            );
        }
    }
    pipeline
        .download_restart_durable_lead_retry_after
        .insert(HOT, Instant::now() + Duration::from_millis(250));
    let (reply, result) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PauseAll { reply })
        .await;
    result.await.unwrap();
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .is_empty()
    );
    let (reply, result) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ResumeAll { reply })
        .await;
    result.await.unwrap();
    pipeline
        .metrics
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);
    pipeline.pause_job_runtime(HOT).unwrap();
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&HOT)
    );
    pipeline.resume_job_runtime(HOT).unwrap();
    let (reply, result) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJob {
            job_id: HOT,
            origin: crate::jobs::handle::CancellationOrigin::User,
            reply,
        })
        .await;
    assert!(result.await.unwrap().is_ok());
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&HOT)
    );
}

#[tokio::test]
async fn checkpoint_abandoned_lane_requeues_once_and_fences_late_events() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    let queued = pipeline.jobs[&HOT].download_queue.len();
    pipeline.dispatch_downloads();
    let (old_id, segment_id) = pipeline.checkpoint_progress_articles[&HOT];
    let original = pipeline.download_lane_owners[&old_id].outstanding[&segment_id].clone();
    pipeline.job_last_download_activity.insert(
        HOT,
        Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );
    pipeline.auto_pause_stalled_downloads();
    assert_eq!(pipeline.jobs[&HOT].status, JobStatus::Paused);
    assert_eq!(pipeline.jobs[&HOT].download_queue.len(), queued);
    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert!(pipeline.rate_limit_reservations.is_empty());
    assert!(pipeline.bandwidth_reservations.is_empty());
    assert!(!pipeline.checkpoint_progress_articles.contains_key(&HOT));
    assert!(!pipeline.download_lane_owners.contains_key(&old_id));
    // Force reuse of the same segment to exercise the hardest stale-event
    // case; ordinary FIFO scheduling may select another queued article first.
    let queue = &mut pipeline.jobs.get_mut(&HOT).unwrap().download_queue;
    let returned = queue
        .pop_first_matching(|work| work.segment_id == segment_id)
        .unwrap();
    let other_work = std::mem::take(queue);
    queue.push(returned);
    pipeline.resume_job_runtime(HOT).unwrap();
    pipeline.dispatch_downloads();
    pipeline.jobs.get_mut(&HOT).unwrap().download_queue = other_work;
    let (replacement, replacement_segment) = pipeline.checkpoint_progress_articles[&HOT];
    assert_ne!(replacement, old_id);
    assert_eq!(replacement_segment, segment_id);
    let reservations = pipeline.rate_limit_reservations.clone();
    let bandwidth = pipeline.bandwidth_reservations.clone();
    let late_result = |data| DownloadResult {
        lane_id: old_id,
        segment_id,
        runtime_generation: pipeline.pool_generation,
        data,
        attempts: vec![],
        lane_observation: None,
        source_server_idx: None,
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: original.retry_count,
        exclude_servers: original.exclude_servers.clone(),
        release_connection_slot: false,
    };
    let late_success = late_result(Ok(DownloadPayload::Raw(Bytes::from_static(
        b"late transport bytes",
    ))));
    let late_error = late_result(Err(DownloadError::fetch(
        DownloadFailureKind::EstablishedTransport,
        "old worker",
    )));
    pipeline.handle_download_done(late_success).await;
    pipeline.handle_download_done(late_error).await;
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        lane_id: old_id,
        job_id: HOT,
        runtime_generation: pipeline.pool_generation,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: true,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility::from_work(&original),
        response_tx,
    });
    assert!(response_rx.await.unwrap().lease.is_none());
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id: old_id,
            results: vec![],
            unrequested_works: vec![original],
            stats: Default::default(),
            ack: None,
        },
        &mut VecDeque::new(),
    );
    pipeline.handle_download_lane_parked(DownloadLaneParked {
        lane_id: old_id,
        job_id: HOT,
        mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        completion_critical: false,
        reason: LaneParkReason::Error,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });
    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(pipeline.rate_limit_reservations, reservations);
    assert_eq!(pipeline.bandwidth_reservations, bandwidth);
    assert_eq!(
        pipeline.checkpoint_progress_articles[&HOT],
        (replacement, segment_id)
    );
    assert_eq!(pipeline.jobs[&HOT].download_queue.len(), queued - 1);
    assert_eq!(pipeline.pending_retries_by_job[&HOT], 130);
    assert_eq!(
        pipeline.download_lane_owners[&replacement].outstanding[&segment_id].retry_count,
        0
    );
}

const HOT: JobId = JobId(49001);
const PEER: JobId = JobId(49002);

#[tokio::test]
async fn checkpoint_teardown_refunds_before_clearing_and_leaves_no_activity() {
    for cancel in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let mut pipeline = restored_with_parked_retries(&temp).await;
        pipeline.dispatch_downloads();
        let (lane_id, segment_id) = pipeline.checkpoint_progress_articles[&HOT];
        let original = pipeline.download_lane_owners[&lane_id].outstanding[&segment_id].clone();
        insert_active_job(
            &mut pipeline,
            PEER,
            segmented_job_spec("Ready peer", "peer.bin", &[100; 64]),
        )
        .await;
        let reserved = pipeline.rate_limit_reservations[&segment_id];
        // Keep a small unrelated debt after the refund: both a missing refund
        // and a duplicate refund produce a materially different balance.
        pipeline.rate_limiter.set_rate(1);
        pipeline.rate_limiter.consume(reserved + 100);
        if cancel {
            let (reply, done) = oneshot::channel();
            pipeline
                .handle_command(SchedulerCommand::CancelJob {
                    job_id: HOT,
                    origin: crate::jobs::handle::CancellationOrigin::User,
                    reply,
                })
                .await;
            done.await.unwrap().unwrap();
        } else {
            pipeline.jobs.get_mut(&HOT).unwrap().status = JobStatus::Failed {
                error: "removed".into(),
            };
            pipeline.purge_terminal_job_runtime(HOT);
        }
        assert!(!pipeline.jobs.contains_key(&HOT));
        assert!(!pipeline.job_last_download_activity.contains_key(&HOT));
        assert!(!pipeline.download_lane_owners.contains_key(&lane_id));
        assert!(pipeline.rate_limit_reservations.is_empty());
        assert!(pipeline.bandwidth_reservations.is_empty());
        assert_eq!(pipeline.active_downloads, 0);
        assert_eq!(pipeline.active_download_connections, 0);
        let remaining = pipeline.rate_limiter.time_until_ready().as_secs_f64();
        assert!(
            (95.0..=100.0).contains(&remaining),
            "refund debt: {remaining}"
        );
        for data in [
            Ok(DownloadPayload::Raw(Bytes::from_static(b"late bytes"))),
            Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "late error",
            )),
        ] {
            pipeline
                .handle_download_done(DownloadResult {
                    lane_id,
                    segment_id,
                    runtime_generation: pipeline.pool_generation,
                    data,
                    attempts: vec![],
                    lane_observation: None,
                    source_server_idx: None,
                    origin: DownloadResultOrigin::NormalPrimary,
                    retry_count: original.retry_count,
                    exclude_servers: original.exclude_servers.clone(),
                    release_connection_slot: true,
                })
                .await;
        }
        let (response_tx, response_rx) = oneshot::channel();
        pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
            lane_id,
            job_id: HOT,
            runtime_generation: pipeline.pool_generation,
            server_idx: 0,
            remote_ip: "127.0.0.1".parse().unwrap(),
            supports_pipelining: true,
            current_mode: DownloadLaneMode::Sequential,
            spillover_loan_kind: None,
            compatibility: DownloadBatchCompatibility::from_work(&original),
            response_tx,
        });
        assert!(response_rx.await.unwrap().lease.is_none());
        pipeline.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::BatchComplete {
                lane_id,
                results: vec![],
                unrequested_works: vec![original],
                stats: Default::default(),
                ack: None,
            },
            &mut VecDeque::new(),
        );
        pipeline.handle_download_lane_parked(DownloadLaneParked {
            lane_id,
            job_id: HOT,
            mode: DownloadLaneMode::Sequential,
            spillover_loan_kind: None,
            completion_critical: false,
            reason: LaneParkReason::Error,
            release_connection_slot: true,
            release_ip_replacement_burst: false,
        });
        let remaining = pipeline.rate_limiter.time_until_ready().as_secs_f64();
        // Retired results still charge their ten actual transport bytes.
        assert!(
            (105.0..=110.0).contains(&remaining),
            "late events must only charge actual transport: {remaining}"
        );
        assert!(!pipeline.job_last_download_activity.contains_key(&HOT));
        assert!(!pipeline.pending_retries_by_job.contains_key(&HOT));
        assert_eq!(pipeline.active_downloads, 0);
        assert_eq!(pipeline.active_download_connections, 0);
        pipeline.rate_limiter.set_rate(0);
        pipeline.dispatch_downloads();
        assert!(pipeline.active_download_connections_by_job[&PEER] > 0);
    }
}

#[tokio::test]
async fn checkpoint_accepted_result_survives_pause_and_lane_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    let queued = pipeline.jobs[&HOT].download_queue.len();
    pipeline.dispatch_downloads();
    let (lane_id, segment_id) = pipeline.checkpoint_progress_articles[&HOT];
    pipeline.pause_job_runtime(HOT).unwrap();
    assert_eq!(
        pipeline.active_downloads, 1,
        "ordinary pause preserves in-flight ownership"
    );
    assert!(pipeline.download_lane_owners.contains_key(&lane_id));
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id,
            results: vec![DownloadResult {
                lane_id,
                segment_id,
                runtime_generation: pipeline.pool_generation,
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::LaneUnavailable,
                    "accepted before retirement",
                )),
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: vec![],
                release_connection_slot: false,
            }],
            unrequested_works: vec![],
            stats: Default::default(),
            ack: None,
        },
        &mut pending,
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pipeline.retire_stalled_download_lanes(HOT), 0);
    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(pipeline.jobs[&HOT].download_queue.len(), queued - 1);
    assert_eq!(
        pipeline.checkpoint_progress_articles[&HOT],
        (lane_id, segment_id)
    );
    pipeline
        .process_released_download_done(pending.pop_front().unwrap())
        .await;
    assert!(!pipeline.checkpoint_progress_articles.contains_key(&HOT));
    assert_eq!(pipeline.pending_retries_by_job[&HOT], 131);
    assert!(
        !pipeline
            .pending_released_download_results_by_job
            .contains_key(&HOT)
    );
}

async fn restored_with_parked_retries(temp: &TempDir) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        temp,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    insert_active_job(
        &mut pipeline,
        HOT,
        segmented_job_spec("Checkpoint progress", "hot.bin", &[1024 * 1024; 2048]),
    )
    .await;
    let state = pipeline.jobs.get_mut(&HOT).unwrap();
    state.restored_download_floor_bytes = 2904;
    state.downloaded_bytes = Pipeline::restart_durable_lead_limit_bytes() + 2904 + 1024 * 1024;
    for _ in 0..130 {
        park_one(&mut pipeline, None);
    }
    pipeline.hot_dispatch_job = Some(HOT);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(30));
    pipeline
}

fn park_one(pipeline: &mut Pipeline, delay: Option<Duration>) {
    let work = pipeline
        .jobs
        .get_mut(&HOT)
        .unwrap()
        .download_queue
        .pop_first_matching(|_| true)
        .unwrap();
    pipeline.note_retry_scheduled(work.segment_id);
    pipeline.schedule_infrastructure_retry(
        delay,
        RetryWork {
            scheduled_pool_generation: pipeline.pool_generation,
            infrastructure_retry: true,
            work,
        },
    );
}

#[tokio::test]
async fn checkpoint_progress_breaks_parked_retry_cycle_and_lends_connections() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    insert_active_job(
        &mut pipeline,
        PEER,
        segmented_job_spec("Ready peer", "peer.bin", &[100; 64]),
    )
    .await;
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads_by_job.get(&HOT), Some(&1));
    assert_eq!(pipeline.pending_retries_by_job.get(&HOT), Some(&130));
    assert_eq!(pipeline.infrastructure_retries.len(), 130);
    assert_eq!(pipeline.infrastructure_retries.next_deadline(), None);
    assert!(!pipeline.job_has_dispatchable_work_for_test(HOT));
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(30));
    for _ in 0..3 {
        pipeline.dispatch_downloads();
    }
    assert_eq!(
        pipeline.active_downloads_by_job.get(&HOT),
        Some(&1),
        "other connections cannot multiply the progress allowance"
    );
    assert!(
        pipeline
            .active_download_connections_by_job
            .get(&PEER)
            .copied()
            .unwrap_or(0)
            > 0,
        "a checkpoint-blocked hot queue must yield spare connections"
    );
    assert_eq!(pipeline.jobs[&HOT].failed_bytes, 0);
}

#[tokio::test]
async fn checkpoint_progress_is_single_article_on_initial_refill_and_trial_paths() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    let pressure = pipeline.refresh_download_pressure();
    let queued = pipeline.jobs[&HOT].download_queue.len();
    // Trials need multiple samples. A failed trial must return its one allowed
    // article, rather than spending or multiplying the progress allowance.
    assert!(
        pipeline
            .try_lease_ip_replacement_trial_batch_for_test(HOT, 0)
            .is_none()
    );
    assert_eq!(pipeline.jobs[&HOT].download_queue.len(), queued);
    let mut lease = pipeline
        .try_lease_initial_download_batch_for_test(HOT, pressure)
        .unwrap();
    assert_eq!(lease.works.len(), 1);
    let work = lease.works.pop().unwrap();
    let initial_id = work.segment_id;
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    pipeline
        .jobs
        .get_mut(&HOT)
        .unwrap()
        .download_queue
        .push(work);
    let mut refill = pipeline
        .try_lease_refill_download_batch_for_test(HOT, compatibility, pressure)
        .unwrap();
    assert_eq!(refill.works.len(), 1);
    let work = refill.works.pop().unwrap();
    let id = work.segment_id;
    assert_eq!(pipeline.jobs[&HOT].download_queue.len(), queued - 1);
    assert_eq!(
        pipeline.jobs[&HOT].download_queue.count_matching(|queued| {
            queued.segment_id == initial_id && queued.retry_count == 0
        }) + usize::from(id == initial_id),
        1,
        "the initial article is preserved across rollback and refill"
    );
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    let remaining = pipeline.jobs[&HOT].download_queue.len();

    // Every stage between activation and actor consumption must reserve the
    // allowance. Parked retries remain present throughout all four stages.
    for stage in 0..4 {
        match stage {
            0 => {
                pipeline.active_downloads_by_job.insert(HOT, 1);
            }
            1 => {
                pipeline.pending_decode.push_back(PendingDecodeWork {
                    segment_id: id,
                    raw: Bytes::from_static(b"pending article"),
                    source_server_idx: None,
                    exclude_servers: Vec::new(),
                });
            }
            2 => {
                pipeline.note_decode_started(id, 1024 * 1024);
            }
            3 => {
                pipeline.note_released_download_result_pending(HOT, 1024 * 1024);
            }
            _ => unreachable!(),
        }
        assert!(
            !pipeline.job_has_dispatchable_work_for_test(HOT),
            "stage {stage}"
        );
        assert!(
            pipeline
                .try_lease_initial_download_batch_for_test(HOT, pressure)
                .is_none()
        );
        assert!(
            pipeline
                .try_lease_refill_download_batch_for_test(HOT, compatibility.clone(), pressure)
                .is_none()
        );
        assert!(
            pipeline
                .try_lease_ip_replacement_trial_batch_for_test(HOT, 0)
                .is_none()
        );
        assert_eq!(pipeline.jobs[&HOT].download_queue.len(), remaining);
        let snapshot = pipeline.diagnostics_snapshot();
        assert_eq!(
            snapshot.jobs[0].checkpoint_admission.as_deref(),
            Some("waiting_for_pipeline")
        );
        pipeline.active_downloads_by_job.clear();
        pipeline.pending_decode.clear();
        pipeline.active_decodes_by_job.clear();
        pipeline.active_decodes_by_file.clear();
        pipeline.active_decode_bytes.clear();
        pipeline.pending_released_download_results_by_job.clear();
        pipeline
            .pending_released_download_result_bytes_by_job
            .clear();
        assert!(
            pipeline.job_has_dispatchable_work_for_test(HOT),
            "stage {stage} drained"
        );
    }
    // An idle cached lane can refill after the prior result has drained;
    // connection ownership and parked retries do not advance durability.
    pipeline.active_download_connections = 4;
    pipeline.active_download_connections_by_job.insert(HOT, 4);
    let cached_refill = pipeline
        .try_lease_refill_download_batch_for_test(HOT, compatibility, pressure)
        .unwrap();
    assert_eq!(cached_refill.works.len(), 1);
    for work in cached_refill.works {
        pipeline
            .jobs
            .get_mut(&HOT)
            .unwrap()
            .download_queue
            .push(work);
    }
    pipeline.active_download_connections = 0;
    pipeline.active_download_connections_by_job.clear();
    pipeline
        .jobs
        .get_mut(&HOT)
        .unwrap()
        .download_queue
        .push(work);
    pipeline
        .persisted_file_progress
        .insert(id.file_id, pipeline.jobs[&HOT].downloaded_bytes);
    let normal = pipeline
        .try_lease_initial_download_batch_for_test(HOT, pressure)
        .unwrap();
    assert!(
        normal.works.len() > 1,
        "durability catch-up restores ordinary batching"
    );
    assert!(normal.works.iter().all(|work| work.retry_count == 0));
    assert_eq!(pipeline.pending_retries_by_job.get(&HOT), Some(&130));
}

#[tokio::test]
async fn checkpoint_progress_keeps_recovery_and_global_pressure_rules() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    pipeline.global_paused = true;
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads, 0);
    pipeline.global_paused = false;
    pipeline
        .metrics
        .write_buffered_bytes
        .store(u64::MAX, Ordering::Relaxed);
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads, 0);
    pipeline
        .metrics
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);
    let pressure = pipeline.refresh_download_pressure();
    pipeline.active_decodes_by_job.insert(HOT, 1);
    assert!(!pipeline.job_has_dispatchable_work_for_test(HOT));
    // Model promoted recovery behind a blocked critical primary article.
    // Recovery is exempt from the durable-lead gate, including on refill class changes.
    let mut recovery = pipeline
        .jobs
        .get_mut(&HOT)
        .unwrap()
        .download_queue
        .pop_first_matching(|_| true)
        .unwrap();
    let payload_compatibility = DownloadBatchCompatibility::from_work(&recovery);
    recovery.is_recovery = true;
    recovery.completion_critical = true;
    let queue = &mut pipeline.jobs.get_mut(&HOT).unwrap().download_queue;
    let mut critical_primary = queue.pop_first_matching(|_| true).unwrap();
    critical_primary.completion_critical = true;
    queue.push(critical_primary);
    pipeline
        .jobs
        .get_mut(&HOT)
        .unwrap()
        .download_queue
        .push(recovery);
    assert!(
        pipeline.jobs[&HOT]
            .download_queue
            .peek_next_matching(|work| !work.is_recovery)
            .is_some(),
        "blocked payload remains ahead of recovery in the queue"
    );
    assert!(pipeline.job_has_dispatchable_work_for_test(HOT));
    let lease = pipeline
        .try_lease_refill_download_batch_for_test(HOT, payload_compatibility, pressure)
        .unwrap();
    assert_eq!(lease.works.len(), 1);
    assert!(lease.works[0].is_recovery);
    assert!(lease.works[0].completion_critical);
}

#[tokio::test]
async fn checkpoint_progress_diagnostics_track_staggered_retries_and_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    park_one(&mut pipeline, Some(Duration::from_secs(60)));
    park_one(&mut pipeline, Some(Duration::from_secs(180)));
    let snapshot = pipeline.diagnostics_snapshot();
    let job = &snapshot.jobs[0];
    assert_eq!(
        job.accepted_download_bytes,
        Some(pipeline.jobs[&HOT].downloaded_bytes)
    );
    assert_eq!(job.durable_download_floor_bytes, Some(2904));
    assert_eq!(
        job.projected_undurable_download_bytes,
        Some(Pipeline::restart_durable_lead_limit_bytes() + 2 * 1024 * 1024)
    );
    assert_eq!(
        job.checkpoint_admission.as_deref(),
        Some("progress_article")
    );
    assert_eq!(job.infrastructure_retries_timed, Some(2));
    assert_eq!(job.infrastructure_retries_indefinite, Some(130));
    let deadline = job.infrastructure_retry_next_deadline_epoch_ms.unwrap();
    assert!(
        deadline > snapshot.captured_at_epoch_ms
            && deadline < snapshot.captured_at_epoch_ms + 61_500.0
    );
    assert_eq!(
        pipeline.infrastructure_retries.len(),
        132,
        "snapshot must be read-only"
    );

    let mut legacy = serde_json::to_value(&snapshot).unwrap();
    for name in [
        "accepted_download_bytes",
        "durable_download_floor_bytes",
        "projected_undurable_download_bytes",
        "checkpoint_lead_limit_bytes",
        "checkpoint_admission",
        "infrastructure_retries_timed",
        "infrastructure_retries_indefinite",
        "infrastructure_retry_next_deadline_epoch_ms",
    ] {
        legacy["jobs"][0].as_object_mut().unwrap().remove(name);
    }
    let legacy: crate::pipeline::diagnostics::PipelineDiagnostics =
        serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.jobs[0].checkpoint_admission, None);
    assert_eq!(legacy.jobs[0].accepted_download_bytes, None);

    let first_deadline = pipeline.infrastructure_retries.next_deadline().unwrap();
    let ready = pipeline.infrastructure_retries.take_due(first_deadline);
    assert_eq!(ready.len(), 1);
    let ready_id = ready[0].work.segment_id;
    for retry in ready {
        pipeline.receive_retry_work(retry);
    }
    assert_eq!(pipeline.pending_retries_by_job.get(&HOT), Some(&131));
    assert_eq!(
        pipeline.jobs[&HOT]
            .download_queue
            .count_matching(|work| work.segment_id == ready_id && work.retry_count == 0),
        1
    );
    assert_eq!(
        pipeline.diagnostics_snapshot().jobs[0].infrastructure_retries_timed,
        Some(1)
    );
    pipeline.dispatch_downloads();
    assert!(pipeline.checkpoint_progress_articles.contains_key(&HOT));
    let (reply, result) = tokio::sync::oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJob {
            job_id: HOT,
            origin: crate::jobs::handle::CancellationOrigin::User,
            reply,
        })
        .await;
    assert!(result.await.unwrap().is_ok());
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert!(!pipeline.pending_retries_by_job.contains_key(&HOT));
    assert!(!pipeline.checkpoint_progress_articles.contains_key(&HOT));
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&HOT)
    );
}

#[tokio::test]
async fn checkpoint_progress_waits_for_result_even_when_durability_catches_up() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp).await;
    pipeline.dispatch_downloads();
    let (lane_id, segment_id) = pipeline.checkpoint_progress_articles[&HOT];
    pipeline
        .persisted_file_progress
        .insert(segment_id.file_id, pipeline.jobs[&HOT].downloaded_bytes);
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads_by_job[&HOT], 1);
    assert!(!pipeline.job_has_dispatchable_work_for_test(HOT));

    // A transport failure is still a processed result: refund the allowance
    // and park the same work without spending its content-retry budget.
    let result = DownloadResult {
        lane_id,
        runtime_generation: pipeline.pool_generation,
        segment_id,
        data: Err(DownloadError::fetch(
            DownloadFailureKind::LaneUnavailable,
            "synthetic unavailable lane",
        )),
        attempts: vec![],
        lane_observation: None,
        source_server_idx: None,
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: vec![],
        release_connection_slot: true,
    };
    pipeline.release_download_result(&result);
    pipeline.note_released_download_result_pending(HOT, 0);
    let pressure = pipeline.refresh_download_pressure();
    assert!(
        pipeline
            .try_lease_initial_download_batch_for_test(HOT, pressure)
            .is_none()
    );
    pipeline.process_released_download_done(result).await;
    assert!(!pipeline.checkpoint_progress_articles.contains_key(&HOT));
    let lease = pipeline
        .try_lease_initial_download_batch_for_test(HOT, pressure)
        .unwrap();
    assert!(lease.works.len() > 1);
    assert_eq!(pipeline.pending_retries_by_job[&HOT], 131);
    assert_eq!(
        pipeline
            .infrastructure_retries
            .iter_with_deadlines()
            .filter(|(_, retry)| {
                retry.work.segment_id == segment_id && retry.work.retry_count == 0
            })
            .count(),
        1
    );
}
