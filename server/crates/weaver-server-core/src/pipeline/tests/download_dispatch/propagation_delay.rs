//! propagation delay

use super::*;

#[tokio::test]
async fn owned_download_lane_capacity_failure_requeues_without_async_fallback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20026);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Owned Capacity Requeue", "owned-capacity.bin", &[1024]),
    )
    .await;
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let segment_id = work.segment_id;
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    let lease = DownloadBatchLease {
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        server_modes: Vec::new(),
        compatibility,
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };
    pipeline
        .reserve_bandwidth_for_dispatch(segment_id, 1024)
        .unwrap();
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);

    let resets_before = pipeline.owned_download_lane_pool.reset_calls();
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        },
        &mut pending,
    );

    assert!(pending.is_empty());
    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(pipeline.active_downloads_by_job.get(&job_id), None);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&job_id),
        None
    );
    assert_eq!(
        pipeline.active_downloads_by_file.get(&segment_id.file_id),
        None
    );
    assert_eq!(
        pipeline.owned_download_lane_pool.reset_calls(),
        resets_before
    );
    let restored = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(restored.segment_id, segment_id);
    assert_eq!(restored.retry_count, 0);
}

/// Health-mutex contention is not a tiering verdict. Before it had its own
/// variant it arrived as `NoEligibleServer`, which sends the lease down the
/// async-fallback arm and resets the owned lane pool — churning a healthy
/// cached TLS lane over a microsecond-long lock collision.
#[tokio::test]
async fn owned_download_lane_selection_contention_requeues_without_async_fallback() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20027);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Owned Contention Requeue", "owned-contention.bin", &[1024]),
    )
    .await;
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let segment_id = work.segment_id;
    let compatibility = DownloadBatchCompatibility::from_work(&work);
    let lease = DownloadBatchLease {
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        server_modes: Vec::new(),
        compatibility,
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };
    pipeline
        .reserve_bandwidth_for_dispatch(segment_id, 1024)
        .unwrap();
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);

    let resets_before = pipeline.owned_download_lane_pool.reset_calls();
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended,
        },
        &mut pending,
    );

    assert!(
        pending.is_empty(),
        "contention must not spawn an async fallback batch"
    );
    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(pipeline.active_downloads_by_job.get(&job_id), None);
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&job_id),
        None
    );
    assert_eq!(
        pipeline.active_downloads_by_file.get(&segment_id.file_id),
        None
    );
    assert_eq!(
        pipeline.owned_download_lane_pool.reset_calls(),
        resets_before,
        "a contended selection must not tear down the owned lane pool"
    );
    let restored = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(restored.segment_id, segment_id);
    assert_eq!(
        restored.retry_count, 0,
        "contention must not consume a download retry"
    );
}

#[test]
fn hot_throughput_window_uses_fixed_two_second_bucketed_rate() {
    let now = Instant::now();
    let mut window = HotJobThroughputWindow::default();

    window.record(now, 2_000);

    assert_eq!(window.bps(now), 1_000);
    assert_eq!(window.bps(now + Duration::from_millis(199)), 1_000);

    window.record(now + Duration::from_millis(250), 2_000);

    assert_eq!(window.bps(now + Duration::from_secs(1)), 2_000);
    assert_eq!(window.bps(now + Duration::from_millis(2_201)), 0);
}

#[test]
fn spillover_loan_book_tracks_multiple_jobs_and_reclaims_independently() {
    let now = Instant::now();
    let mut loans = SpilloverLoanBook::default();
    let first_job = JobId(21001);
    let second_job = JobId(21002);

    loans.start_or_extend(first_job, now, 10_000, SpilloverLoanKind::MeasuredUnderfill);
    loans.start_or_extend(first_job, now, 10_000, SpilloverLoanKind::MeasuredUnderfill);
    loans.start_or_extend(
        second_job,
        now,
        12_000,
        SpilloverLoanKind::MeasuredUnderfill,
    );

    assert_eq!(loans.active_lent_connections(), 3);
    assert_eq!(loans.active_loan_count(), 2);
    assert_eq!(loans.speed_snapshot(), (10_000, 0, 2));
    assert!(!loans.update_speed_harm(now + Duration::from_millis(1_999), 8_000, 7));
    assert!(!loans.reclaim_pending_for(first_job));
    assert!(!loans.reclaim_pending_for(second_job));

    assert!(loans.update_speed_harm(now + Duration::from_secs(2), 8_000, 7));
    assert!(loans.reclaim_pending_for(first_job));
    assert!(loans.reclaim_pending_for(second_job));
    assert_eq!(loans.speed_snapshot(), (10_000, 8_000, 2));

    loans.release_one(first_job, SpilloverLoanKind::MeasuredUnderfill);
    assert!(loans.reclaim_pending_for(first_job));
    loans.release_one(first_job, SpilloverLoanKind::MeasuredUnderfill);
    assert!(!loans.reclaim_pending_for(first_job));
    assert!(loans.reclaim_pending_for(second_job));
    assert_eq!(loans.active_lent_connections(), 1);
    assert_eq!(loans.active_loan_count(), 1);
}

/// New-contract test: `distinct_loan_jobs`/`holds_loan` are what the dispatch
/// arm's two-job cap is built on — extending an existing job's loan never
/// grows the distinct count, and it always agrees with `active_loan_count`.
#[test]
fn spillover_loan_book_distinct_jobs_tracks_active_loan_count() {
    let now = Instant::now();
    let mut loans = SpilloverLoanBook::default();
    let first_job = JobId(21003);
    let second_job = JobId(21004);

    assert_eq!(loans.distinct_loan_jobs(), 0);
    assert!(!loans.holds_loan(first_job));

    loans.start_or_extend(first_job, now, 10_000, SpilloverLoanKind::MeasuredUnderfill);
    assert_eq!(loans.distinct_loan_jobs(), 1);
    assert!(loans.holds_loan(first_job));
    assert!(!loans.holds_loan(second_job));

    // A second connection for the same job does not grow the distinct count.
    loans.start_or_extend(first_job, now, 10_000, SpilloverLoanKind::MeasuredUnderfill);
    assert_eq!(loans.distinct_loan_jobs(), 1);
    assert_eq!(loans.active_lent_connections(), 2);

    loans.start_or_extend(
        second_job,
        now,
        10_000,
        SpilloverLoanKind::MeasuredUnderfill,
    );
    assert_eq!(loans.distinct_loan_jobs(), 2);
    assert_eq!(loans.distinct_loan_jobs(), loans.active_loan_count());
    assert!(loans.holds_loan(second_job));

    loans.release_one(first_job, SpilloverLoanKind::MeasuredUnderfill);
    assert_eq!(loans.distinct_loan_jobs(), 2);
    assert!(loans.holds_loan(first_job));
    loans.release_one(first_job, SpilloverLoanKind::MeasuredUnderfill);
    assert_eq!(loans.distinct_loan_jobs(), 1);
    assert!(!loans.holds_loan(first_job));
}

// bounded_same_band_does_not_reclaim_on_hot_only_speed_drop and
// spillover_loan_book_releases_mixed_kinds_independently removed:
// `SpilloverLoanKind::BoundedSameBand` no longer exists — the loan book
// tracks only `MeasuredUnderfill` loans now, so there is no second kind left
// to mix or to exempt from speed-harm reclaim. Per-job tracking, release,
// and reclaim independence with the sole remaining kind are still covered by
// `spillover_loan_book_tracks_multiple_jobs_and_reclaims_independently`, and
// `spillover_loan_book_distinct_jobs_tracks_active_loan_count` above covers
// the replacement for bounded sharing: a hard cap on distinct spilled-to jobs
// (see `spillover_caps_distinct_jobs_at_two_even_with_capacity_to_spare`
// for the dispatch-level enforcement) instead of a per-kind connection share.

#[tokio::test]
async fn ip_replacement_policy_stop_is_neutral_and_lossless() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21050);
    let first_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let tail_segment = SegmentId {
        segment_number: 1,
        ..first_segment
    };
    let spec = segmented_job_spec("IP Policy Stop", "ip-policy.bin", &[128, 64]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 2;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 2);

    let quota_data: std::result::Result<DownloadPayload, DownloadError> =
        Err(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::ServerQuota,
            "server quota stopped the IP replacement trial",
        )));
    let unrequested_data: std::result::Result<DownloadPayload, DownloadError> =
        Err(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::Unrequested,
            "tail was not requested after the policy stop",
        )));
    assert!(crate::pipeline::download::is_ip_replacement_policy_stop(
        &quota_data
    ));
    assert!(crate::pipeline::download::is_ip_replacement_policy_stop(
        &unrequested_data
    ));
    assert!(
        crate::pipeline::download::should_neutrally_park_ip_replacement(
            true,
            LaneParkReason::NoWork,
        )
    );
    assert!(
        !crate::pipeline::download::should_neutrally_park_ip_replacement(
            true,
            LaneParkReason::Error,
        ),
        "an earlier network failure must not be hidden by a later policy stop"
    );

    let rejected_before = pipeline
        .metrics
        .ip_replacement_trials_rejected_total
        .load(Ordering::Relaxed);
    let proof_before = pipeline
        .metrics
        .download_lane_parks_proof_failure_total
        .load(Ordering::Relaxed);
    let errors_before = pipeline
        .metrics
        .download_lane_parks_error_total
        .load(Ordering::Relaxed);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: first_segment,
            data: quota_data,
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::IpReplacementTrial,
            retry_count: 4,
            exclude_servers: vec![2],
            release_connection_slot: false,
        })
        .await;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id: tail_segment,
            data: unrequested_data,
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::IpReplacementTrial,
            retry_count: 7,
            exclude_servers: vec![2],
            release_connection_slot: false,
        })
        .await;

    let mut restored = Vec::new();
    let queue = &mut pipeline.jobs.get_mut(&job_id).unwrap().download_queue;
    while let Some(work) = queue.pop() {
        restored.push(work);
    }
    restored.sort_unstable_by_key(|work| work.segment_id.segment_number);
    assert_eq!(restored.len(), 2);
    assert_eq!(restored[0].segment_id, first_segment);
    assert_eq!(restored[0].retry_count, 4);
    assert_eq!(restored[1].segment_id, tail_segment);
    assert_eq!(restored[1].retry_count, 7);
    assert!(restored.iter().all(|work| work.exclude_servers == vec![2]));
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(!pipeline.server_quota_parked.contains(&first_segment));

    pipeline.ip_replacement_burst_active = true;
    pipeline.metrics.set_ip_replacement_burst_active(true);
    pipeline.handle_download_lane_parked(DownloadLaneParked {
        job_id,
        mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        completion_critical: false,
        reason: LaneParkReason::ServerQuota,
        release_connection_slot: false,
        release_ip_replacement_burst: true,
    });
    assert!(!pipeline.ip_replacement_burst_active);
    assert_eq!(
        pipeline
            .metrics
            .ip_replacement_trials_rejected_total
            .load(Ordering::Relaxed),
        rejected_before
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_parks_proof_failure_total
            .load(Ordering::Relaxed),
        proof_before
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_parks_error_total
            .load(Ordering::Relaxed),
        errors_before
    );
}

#[tokio::test]
async fn release_download_result_excludes_ip_replacement_trial_from_hot_success_and_speed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21003);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.hot_dispatch_job = Some(job_id);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(2));
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    pipeline.release_download_result(&DownloadResult {
        runtime_generation: 0,
        segment_id,
        data: Ok(DownloadPayload::Raw(Bytes::from_static(b"trial-article"))),
        attempts: vec![],
        lane_observation: None,
        source_server_idx: None,
        origin: DownloadResultOrigin::IpReplacementTrial,
        retry_count: 0,
        exclude_servers: vec![],
        release_connection_slot: false,
    });

    assert_eq!(
        pipeline
            .metrics
            .hot_dispatch_hot_speed_bps
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn accepted_ip_replacement_trial_samples_update_per_ip_ewma() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let candidate_ip = "203.0.113.42".parse().unwrap();
    let old_key = ServerIpKey {
        server_idx: 0,
        ip: "203.0.113.7".parse().unwrap(),
    };

    pipeline.ip_replacement_trial_extra_connections = 1;
    pipeline.ip_replacement_burst_active = true;
    pipeline.handle_ip_replacement_trial_event(IpReplacementTrialEvent::CandidateAccepted {
        old_key,
        samples: vec![weaver_nntp::client::FetchAttemptTrace {
            server_idx: 0,
            remote_ip: Some(candidate_ip),
            elapsed: Duration::from_millis(25),
            outcome: weaver_nntp::client::FetchAttemptOutcome::Success,
            error: None,
        }],
    });

    let key = ServerIpKey {
        server_idx: 0,
        ip: candidate_ip,
    };
    let state = pipeline
        .ip_rtt_ewma
        .get(&key)
        .expect("accepted candidate sample should be merged into IP EWMA");
    assert_eq!(state.samples, 1);
    assert_eq!(
        pipeline.metrics.ip_rtt_ewma_entries.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline
            .metrics
            .ip_rtt_ewma_slowest_ms
            .load(Ordering::Relaxed),
        25
    );
    assert!(pipeline.ip_replacement_retired_ips.contains(&old_key));
}

#[tokio::test]
async fn disabled_ip_replacement_ignores_late_candidate_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let candidate_ip = "203.0.113.42".parse().unwrap();
    let old_key = ServerIpKey {
        server_idx: 0,
        ip: "203.0.113.7".parse().unwrap(),
    };

    pipeline.ip_replacement_trial_extra_connections = 0;
    pipeline.ip_replacement_burst_active = false;
    pipeline.handle_ip_replacement_trial_event(IpReplacementTrialEvent::CandidateAccepted {
        old_key,
        samples: vec![weaver_nntp::client::FetchAttemptTrace {
            server_idx: 0,
            remote_ip: Some(candidate_ip),
            elapsed: Duration::from_millis(25),
            outcome: weaver_nntp::client::FetchAttemptOutcome::Success,
            error: None,
        }],
    });

    assert!(pipeline.ip_replacement_retired_ips.is_empty());
    assert!(pipeline.ip_rtt_ewma.is_empty());
    assert_eq!(
        pipeline.metrics.ip_rtt_ewma_entries.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .ip_rtt_ewma_slowest_ms
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn retired_ip_replacement_lane_parks_at_refill_boundary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let old_key = ServerIpKey {
        server_idx: 0,
        ip: "203.0.113.7".parse().unwrap(),
    };
    pipeline.ip_replacement_retired_ips.insert(old_key);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id: JobId(21004),
        server_idx: old_key.server_idx,
        remote_ip: Some(old_key.ip),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: 3,
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
    assert_eq!(response.park_reason, LaneParkReason::IpReplacementRetired);
}

#[tokio::test]
async fn ip_replacement_trial_starts_when_every_connection_is_busy() {
    let temp_dir = tempfile::tempdir().unwrap();
    let configured_download_capacity = 4;
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: configured_download_capacity,
            medium_count: 1,
            large_count: 1,
        },
        configured_download_capacity,
    )
    .await;
    pipeline.ip_replacement_trial_extra_connections = 1;

    // Nothing is held back from the ordinary budget any more, so the trial's
    // precondition is simply that every configured connection is busy.
    let normal_download_capacity = configured_download_capacity;
    assert!(normal_download_capacity > 0);

    let job_id = JobId(21005);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec(
            "IP Replacement Hot Job",
            "ip-replacement.bin",
            &[512, 512, 512, 512],
        ),
    )
    .await;

    let now = Instant::now();
    let slow_key = ServerIpKey {
        server_idx: 0,
        ip: "203.0.113.7".parse().unwrap(),
    };
    let baseline_key = ServerIpKey {
        server_idx: 0,
        ip: "203.0.113.8".parse().unwrap(),
    };
    pipeline.ip_rtt_ewma.insert(
        slow_key,
        IpRttEwma {
            ewma_ms: 250.0,
            samples: 16,
            first_seen: now - Duration::from_secs(31),
            last_seen: now,
        },
    );
    pipeline.ip_rtt_ewma.insert(
        baseline_key,
        IpRttEwma {
            ewma_ms: 100.0,
            samples: 8,
            first_seen: now - Duration::from_secs(31),
            last_seen: now,
        },
    );

    pipeline.active_download_connections = normal_download_capacity;
    pipeline.dispatch_downloads();

    assert!(pipeline.ip_replacement_burst_active);
    assert_eq!(
        pipeline
            .metrics
            .ip_replacement_trials_started_total
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline
            .metrics
            .ip_replacement_trials_blocked_total
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn dispatch_downloads_leases_hot_job_batch_before_same_band_spillover() {
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

    let hot_job_id = JobId(20023);
    let secondary_job_id = JobId(20024);

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Hot Normal",
                &[
                    ("hot-0.bin".to_string(), 512u32),
                    ("hot-1.bin".to_string(), 512u32),
                    ("hot-2.bin".to_string(), 512u32),
                ],
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        secondary_job_id,
        with_priority(
            standalone_job_spec("Secondary Normal", &[("secondary.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 3);
    // Both of the hot job's own lanes carry its three articles: a lease is
    // bounded by one lane's fair share of the job's remainder, so the tail
    // never lands on a single connection while a second one sits idle.
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(
        pipeline.jobs.get(&hot_job_id).unwrap().download_queue.len(),
        0
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&secondary_job_id)
            .unwrap()
            .download_queue
            .len(),
        1
    );
    assert_eq!(pipeline.active_downloads_by_job.get(&hot_job_id), Some(&3));
    assert!(
        !pipeline
            .active_downloads_by_job
            .contains_key(&secondary_job_id)
    );
}

#[tokio::test]
async fn dispatch_downloads_throttles_hot_job_under_soft_byte_pressure() {
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
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);

    let hot_job_id = JobId(20029);
    let secondary_job_id = JobId(20030);

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec(
                "Soft Pressure Hot Full",
                &[
                    ("hot-0.bin".to_string(), 512u32),
                    ("hot-1.bin".to_string(), 512u32),
                    ("hot-2.bin".to_string(), 512u32),
                ],
            ),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        secondary_job_id,
        with_priority(
            standalone_job_spec(
                "Soft Pressure Secondary Parked",
                &[("secondary.bin".to_string(), 512u32)],
            ),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(pipeline.active_downloads_by_job.get(&hot_job_id), Some(&1));
    assert!(
        !pipeline
            .active_downloads_by_job
            .contains_key(&secondary_job_id)
    );
    assert_eq!(
        pipeline.jobs.get(&hot_job_id).unwrap().download_queue.len(),
        2
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&secondary_job_id)
            .unwrap()
            .download_queue
            .len(),
        1
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Soft.as_code()
    );
    assert!(pipeline.download_pressure_soft_dispatch_after.is_some());

    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(
        pipeline.jobs.get(&hot_job_id).unwrap().download_queue.len(),
        2
    );

    pipeline.download_pressure_soft_dispatch_after =
        Some(Instant::now() - Duration::from_millis(1));
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.active_downloads, 2);
    assert_eq!(pipeline.active_download_connections, 2);
    assert_eq!(pipeline.active_downloads_by_job.get(&hot_job_id), Some(&2));
    assert_eq!(
        pipeline.jobs.get(&hot_job_id).unwrap().download_queue.len(),
        1
    );
}

#[tokio::test]
async fn dispatch_downloads_suppresses_spillover_under_soft_byte_pressure() {
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
    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);

    let hot_job_id = JobId(20025);
    let secondary_job_id = JobId(20026);

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Soft Pressure Hot", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        secondary_job_id,
        with_priority(
            standalone_job_spec(
                "Soft Pressure Secondary",
                &[("secondary.bin".to_string(), 512u32)],
            ),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.active_downloads_by_job.get(&hot_job_id), Some(&1));
    assert!(
        !pipeline
            .active_downloads_by_job
            .contains_key(&secondary_job_id)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&secondary_job_id)
            .unwrap()
            .download_queue
            .len(),
        1
    );
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
}

#[tokio::test]
async fn dispatch_downloads_reorders_after_priority_metadata_change() {
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

    let leading_job_id = JobId(20021);
    let promoted_job_id = JobId(20022);
    let leading_files = (0..66)
        .map(|idx| (format!("lead-{idx}.bin"), 512u32))
        .collect::<Vec<_>>();

    insert_active_job(
        &mut pipeline,
        leading_job_id,
        with_priority(
            standalone_job_spec("Leading Low Priority", &leading_files),
            "LOW",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        promoted_job_id,
        with_priority(
            standalone_job_spec("Promoted Priority", &[("promoted.bin".to_string(), 512u32)]),
            "LOW",
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&leading_job_id).unwrap();
        let _ = state.download_queue.pop().expect(
            "leading low-priority job should have one completed segment removed from queue",
        );
        state
            .assembly
            .file_mut(NzbFileId {
                job_id: leading_job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 512)
            .unwrap();
    }

    pipeline.dispatch_downloads();

    // 65 queued articles minus the 16-article cold-start first-wave lease.
    assert_eq!(
        pipeline
            .jobs
            .get(&leading_job_id)
            .unwrap()
            .download_queue
            .len(),
        49
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&promoted_job_id)
            .unwrap()
            .download_queue
            .len(),
        1
    );

    pipeline.active_downloads = 0;
    pipeline.active_download_connections = 0;
    pipeline.active_downloads_by_job.clear();
    pipeline.active_download_connections_by_job.clear();
    pipeline.active_download_passes.clear();
    pipeline
        .jobs
        .get_mut(&promoted_job_id)
        .unwrap()
        .spec
        .metadata = vec![("priority".to_string(), "NORMAL".to_string())];

    pipeline.dispatch_downloads();

    // The promoted job takes the lane; the leading job's queue is untouched.
    assert_eq!(
        pipeline
            .jobs
            .get(&leading_job_id)
            .unwrap()
            .download_queue
            .len(),
        49
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&promoted_job_id)
            .unwrap()
            .download_queue
            .len(),
        0
    );
}

#[tokio::test]
async fn list_jobs_projects_downloading_while_extracting_with_active_download_work() {
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
    let job_id = JobId(20008);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Extracting Still Downloading",
            &[("queued.bin".to_string(), 512)],
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        assert_eq!(
            state.download_state,
            crate::jobs::model::DownloadState::Complete
        );
        assert_eq!(state.download_queue.len(), 1);
    }

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    assert_eq!(info.status, JobStatus::Extracting);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Complete
    );
    assert_eq!(info.post_state, crate::jobs::model::PostState::Extracting);

    pipeline.active_downloads_by_job.insert(job_id, 1);
    let active_info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    assert_eq!(
        active_info.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
}

#[tokio::test]
async fn list_jobs_queues_inactive_downloads_and_rewarms_their_phase_rate() {
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
    let job_id = JobId(20009);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Inactive Download", &[("queued.bin".to_string(), 512)]),
    )
    .await;

    pipeline.phase_begin(job_id, JobPhase::Downloading, Some(512));
    assert!(!pipeline.job_has_current_download_activity(job_id));

    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline.sample_phase_progress();
    {
        let runtime = pipeline
            .phase_progress
            .get_mut(&(job_id, JobPhase::Downloading))
            .expect("download phase should exist");
        runtime.first_sample_at = Some(std::time::Instant::now() - Duration::from_secs(10));
        runtime.rate.update(std::time::Instant::now(), 64);
        assert!(runtime.rate.has_samples());
    }

    pipeline.active_downloads_by_job.remove(&job_id);
    pipeline.sample_phase_progress();
    let inactive_info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    assert_eq!(
        inactive_info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert!(inactive_info.phase_progress.is_empty());
    let runtime = pipeline
        .phase_progress
        .get(&(job_id, JobPhase::Downloading))
        .expect("download phase should remain registered");
    assert!(!runtime.rate.has_samples());
    assert!(runtime.first_sample_at.is_none());

    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline.sample_phase_progress();
    let active_info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    assert_eq!(
        active_info.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(active_info.phase_progress.len(), 1);
    assert_eq!(active_info.phase_progress[0].phase, JobPhase::Downloading);
    assert!(active_info.phase_progress[0].rate_bps.is_none());
}

#[tokio::test]
async fn download_phase_rate_matches_the_global_speed_gauge() {
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
    let job_id = JobId(20010);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Wire Rate", &[("payload.bin".to_string(), 1_024)]),
    )
    .await;
    pipeline.phase_begin(job_id, JobPhase::Downloading, Some(1_024));
    pipeline.active_downloads_by_job.insert(job_id, 1);

    // One tick: both estimators take their first sample together. The gauge
    // already sampled once when the pipeline was built, which would start its
    // window early and skew the comparison, so it is emptied first.
    pipeline.metrics.reset_speed_tracker();
    pipeline.sample_phase_progress();
    pipeline.shared_state.refresh_metrics_snapshot();

    // Decoded bytes move the bar but never the rate: the row's rate must
    // integrate the same wire bytes the nav counter does.
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes = 512;
    tokio::time::sleep(Duration::from_millis(300)).await;
    pipeline.sample_phase_progress();
    pipeline.shared_state.refresh_metrics_snapshot();
    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    assert_eq!(info.phase_progress.len(), 1);
    assert_eq!(info.phase_progress[0].completed_bytes, 512);
    assert!(info.phase_progress[0].rate_bps.is_none());
    assert_eq!(
        pipeline
            .shared_state
            .metrics_snapshot()
            .current_download_speed,
        0
    );

    // The same wire bytes land on both counters; the next tick must publish
    // the same number on the row and the gauge.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .downloaded_wire_bytes = 30_000;
    pipeline
        .metrics
        .bytes_downloaded
        .fetch_add(30_000, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    pipeline.sample_phase_progress();
    pipeline.shared_state.refresh_metrics_snapshot();
    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");
    let row = info.phase_progress[0]
        .rate_bps
        .expect("wire bytes should produce a rate");
    let gauge = pipeline
        .shared_state
        .metrics_snapshot()
        .current_download_speed;
    assert!(gauge > 0, "gauge should be live");
    let tolerance = (gauge as f64 * 0.02).max(2.0);
    assert!(
        (row as f64 - gauge as f64).abs() <= tolerance,
        "row rate {row} should match gauge {gauge} within {tolerance}"
    );
    assert_eq!(info.phase_progress[0].completed_bytes, 512);
}

#[tokio::test]
async fn dispatch_downloads_blocks_when_isp_bandwidth_cap_is_hit() {
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
    let job_id = JobId(20005);
    let spec = standalone_job_spec("ISP Cap Gate", &[("queued.bin".to_string(), 512u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

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

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 1);
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::IspCap
    );
}

#[tokio::test]
async fn server_quota_ui_state_does_not_globally_stop_unrelated_dispatch() {
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
    let job_id = JobId(20996);
    let spec = standalone_job_spec("Server Quota Gate", &[("queued.bin".to_string(), 512u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.shared_state.set_server_quota_blocked(true);
    pipeline
        .shared_state
        .set_download_block(crate::jobs::handle::DownloadBlockState {
            kind: crate::jobs::handle::DownloadBlockKind::Scheduled,
            scheduled_speed_limit: 1_024,
            ..Default::default()
        });
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );
    pipeline.shared_state.set_server_quota_blocked(false);
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::Scheduled
    );
    pipeline.shared_state.set_server_quota_blocked(true);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 0);
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );
}

#[tokio::test]
async fn bandwidth_cap_state_refresh_preserves_scheduled_speed_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    pipeline.scheduled_rate_limit = Some(131_072);
    pipeline.rate_limiter.set_rate(131_072);
    pipeline.refresh_bandwidth_cap_window().unwrap();
    assert_eq!(
        pipeline.shared_state.download_block().scheduled_speed_limit,
        131_072
    );

    pipeline.record_download_bandwidth_usage(1_024).unwrap();
    assert_eq!(
        pipeline.shared_state.download_block().scheduled_speed_limit,
        131_072
    );

    pipeline.flush_download_bandwidth_usage().unwrap();
    assert_eq!(
        pipeline.shared_state.download_block().scheduled_speed_limit,
        131_072
    );
}

#[tokio::test]
async fn clearing_scheduled_speed_limit_restores_latest_configured_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::SetSpeedLimit {
            bytes_per_sec: 512 * 1024,
            reply,
        })
        .await;
    received.await.unwrap();

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ApplyScheduleAction {
            action: crate::bandwidth::ScheduleAction::SpeedLimit {
                bytes_per_sec: 128 * 1024,
            },
            reply,
        })
        .await;
    received.await.unwrap();
    assert_eq!(pipeline.rate_limiter.rate(), 128 * 1024);

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::SetSpeedLimit {
            bytes_per_sec: 768 * 1024,
            reply,
        })
        .await;
    received.await.unwrap();
    assert_eq!(pipeline.configured_rate_limit, 768 * 1024);
    assert_eq!(pipeline.rate_limiter.rate(), 128 * 1024);

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ClearScheduleAction { reply })
        .await;
    received.await.unwrap();
    assert_eq!(pipeline.scheduled_rate_limit, None);
    assert_eq!(pipeline.rate_limiter.rate(), 768 * 1024);
    assert_eq!(
        pipeline.shared_state.download_block().scheduled_speed_limit,
        0
    );
}

#[tokio::test]
async fn set_bandwidth_cap_policy_recomputes_current_window_usage_from_ledger() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    let now = chrono::Local::now();
    let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
    pipeline
        .db
        .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 4096)
        .unwrap();

    pipeline
        .apply_bandwidth_cap_policy(Some(crate::bandwidth::IspBandwidthCapConfig {
            enabled: false,
            period: crate::bandwidth::IspBandwidthCapPeriod::Daily,
            limit_bytes: 10_000,
            reset_time_minutes_local: reset_minutes,
            weekly_reset_weekday: crate::bandwidth::IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }))
        .unwrap();

    let block = pipeline.shared_state.download_block();
    assert_eq!(block.used_bytes, 4096);
    assert_eq!(block.remaining_bytes, 10_000 - 4096);
    assert!(!block.cap_enabled);
}

#[tokio::test]
async fn streamed_decoded_download_bypasses_decode_backlog() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20017);
    let filename = "streamed.bin";
    let payload = b"already decoded bytes".to_vec();
    let spec = segmented_job_spec(
        "Streamed Decode Success",
        filename,
        &[payload.len() as u32, 1],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let raw_size = 256;
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Decoded(DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size,
                yenc_layout: YencLayoutAssertions {
                    file_size: payload.len() as u64 + 1,
                    part: Some(1),
                    total: Some(2),
                    begin: Some(1),
                    end: Some(payload.len() as u64),
                },
                crc_valid: true,
                part_crc_verified: false,
                part_crc: par2_rs::checksum::crc32(&payload),
                expected_file_crc: None,
                data: DecodedChunk::from(payload.clone()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            })),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![2],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.pending_decode.len(), 0);
    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.metrics.bytes_downloaded.load(Ordering::Relaxed),
        raw_size
    );
    assert_eq!(
        pipeline.metrics.bytes_decoded.load(Ordering::Relaxed),
        payload.len() as u64
    );
    assert_eq!(pipeline.metrics.segments_decoded.load(Ordering::Relaxed), 1);
    assert!(pipeline.decode_done_rx.try_recv().is_err());
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .map(|state| state.downloaded_bytes),
        Some(payload.len() as u64)
    );
    // Wire bytes are credited to the job in lockstep with the global counter.
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .map(|state| state.downloaded_wire_bytes),
        Some(raw_size)
    );
    let provenance = pipeline
        .unverified_segments
        .get(&segment_id.file_id)
        .and_then(|segments| segments.get(&segment_id.segment_number))
        .expect("streamed success must retain unverified segment provenance");
    assert_eq!(provenance.source_server_idx, Some(0));
    assert_eq!(provenance.exclude_servers, vec![2]);
}

#[tokio::test]
async fn add_job_with_options_initially_paused_publishes_queued_lane_from_real_pipeline() {
    let harness = TestHarness::new().await;
    let job_id = JobId(30037);

    harness
        .handle
        .add_job_with_options(
            job_id,
            standalone_job_spec("Paused From Start", &[("episode.mkv".to_string(), 123)]),
            PathBuf::from(format!("job-{}.nzb", job_id.0)),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions {
                initially_paused: true,
                semantic_materialization_generation: None,
                semantic_promotion_generation: None,
            },
        )
        .await
        .unwrap();

    let info = harness.handle.get_job(job_id).unwrap();
    assert_eq!(info.status, JobStatus::Paused);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert_eq!(info.run_state, crate::jobs::model::RunState::Paused);

    harness.handle.resume_job(job_id).await.unwrap();
    let info = harness.handle.get_job(job_id).unwrap();
    assert_eq!(info.status, JobStatus::Queued);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert_eq!(info.run_state, crate::jobs::model::RunState::Active);

    harness.shutdown().await;
}

#[tokio::test]
async fn redownload_job_rebuilds_failed_history_as_queued_download() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30038);
    let working_dir = intermediate_dir.join("failed-redownload-job");
    let staging_dir = pipeline
        .complete_dir
        .join(".weaver-staging")
        .join(job_id.0.to_string());

    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();
    tokio::fs::write(working_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(staging_dir.join("partial.srt"), b"partial")
        .await
        .unwrap();
    let mut row =
        history_row_with_output_dir(job_id, "Failed History Job", "failed", working_dir.clone());
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Queued);
    assert!(!state.download_queue.is_empty());
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .all(|job| job.job_id != job_id)
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn redownload_job_rebuilds_complete_history_as_queued_download() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30040);
    let working_dir = complete_dir.join("complete-redownload-job");
    let staging_dir = pipeline
        .complete_dir
        .join(".weaver-staging")
        .join(job_id.0.to_string());

    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();
    tokio::fs::create_dir_all(working_dir.join("subs"))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("episode.mkv"), b"complete")
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("subs").join("episode.srt"), b"complete")
        .await
        .unwrap();
    tokio::fs::write(staging_dir.join("partial.srt"), b"partial")
        .await
        .unwrap();
    let mut row = history_row_with_output_dir(
        job_id,
        "Complete History Job",
        "complete",
        working_dir.clone(),
    );
    row.category = Some("tv".to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Queued);
    assert!(!state.download_queue.is_empty());
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .all(|job| job.job_id != job_id)
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn redownload_job_uses_history_blob_when_history_path_is_stale() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30041);
    let stale_nzb_path = temp_dir.path().join("missing-history.nzb");
    let working_dir = intermediate_dir.join("stale-redownload-job");

    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    let mut row =
        history_row_with_output_dir(job_id, "Stale History Job", "failed", working_dir.clone());
    row.category = Some("tv".to_string());
    row.nzb_path = Some(stale_nzb_path.display().to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    insert_history_row_with_nzb_zstd(&pipeline.db, &row, &sample_nzb_zstd());

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Queued);
    assert!(!state.download_queue.is_empty());
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!stale_nzb_path.exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn no_par2_single_file_retry_marks_zip_volume_complete_after_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30028);
    let spec = JobSpec {
        name: "No PAR2 ZIP Retry Refresh".to_string(),
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
                message_id: "zip-refresh-0@example.com".to_string(),
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

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.assembly.archive_topology_for("archive.zip").is_none());
    }

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

    pipeline.try_update_7z_topology(job_id, file_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive.zip").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive.zip"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_single_file_retry_marks_7z_volume_complete_after_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30029);
    let spec = JobSpec {
        name: "No PAR2 7z Retry Refresh".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "archive.7z".to_string(),
            role: FileRole::from_filename("archive.7z"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "7z-refresh-0@example.com".to_string(),
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
            "archive.7z".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([("archive.7z".to_string(), 0)]),
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
        .insert(job_id, HashSet::from(["archive.7z".to_string()]));

    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.assembly.archive_topology_for("archive.7z").is_none());
    }

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

    pipeline.try_update_7z_topology(job_id, file_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive.7z").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive.7z"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn incomplete_download_promotes_parked_par2_metadata_before_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let spec = JobSpec {
        name: "Promote Parked PAR2 Metadata".to_string(),
        password: None,
        total_bytes: 164,
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
                    bytes: 100,
                    message_id: "parked-par2-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "parked-par2-index@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        state.download_queue = DownloadQueue::new();
        for work in queued {
            if work.segment_id.file_id.file_index == 1 {
                state.recovery_queue.push(work);
            }
        }
        state.failed_bytes = 100;
    }

    pipeline.check_job_completion(job_id).await;

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        state
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == 1)
            > 0
    );
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&1))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn direct_payload_par2_repair_completes_after_download_exhaustion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30080);
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
        name: "Direct Payload PAR2 Repair".to_string(),
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
                        message_id: "payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "payload-1@example.com".to_string(),
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
                    message_id: "payload-index@example.com".to_string(),
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
                    message_id: "payload-recovery@example.com".to_string(),
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
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Direct Payload PAR2 Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[test]
fn runtime_transition_failpoint_mapping_uses_lane_transitions() {
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Verifying,
            crate::jobs::model::RunState::Active,
        ),
        Some(crate::e2e_failpoint::STATUS_ENTER_VERIFYING)
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::QueuedRepair,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Repairing,
            crate::jobs::model::RunState::Active,
        ),
        Some(crate::e2e_failpoint::STATUS_ENTER_REPAIRING)
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Paused,
        ),
        Some(crate::e2e_failpoint::STATUS_ENTER_PAUSED)
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Extracting,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::WaitingForVolumes,
            crate::jobs::model::RunState::Active,
        ),
        None
    );
}

#[tokio::test]
async fn no_par2_retry_clears_detected_archive_identity_before_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    let filename = "51273aad56a8b904e96928935278a627";
    let fixture_bytes = rar5_fixture_bytes("rar5_store.rar");
    let spec = rar_job_spec(
        "Obfuscated RAR Retry Clears Detection",
        &[(filename.to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(file.role(), FileRole::Unknown));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some(filename)
        );
    }
    assert_eq!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .len(),
        1
    );

    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([filename.to_string()]));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(state.status, JobStatus::Downloading));
        assert_eq!(state.download_queue.len(), 1);
        assert!(!file.is_complete());
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            FileRole::Unknown
        ));
        assert!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .is_none()
        );
        assert!(state.assembly.archive_topology_for(filename).is_none());
    }
    assert!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_archive_headers(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some(filename)
        );
    }
    assert_eq!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .len(),
        1
    );
}

/// A probe that is still in flight when the last segment settles must not hold
/// the completion checkpoint.
///
/// The probe estimates a release's health from a sample while bytes are still
/// arriving. Once the download pass ends, the job's own terminal states are the
/// answer, and waiting on the probe only postpones the checkpoint — and PAR2
/// recovery promotion with it — for the probe's whole soft timeout.
#[tokio::test]
async fn drained_download_pass_retires_the_probe_instead_of_waiting_for_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec(
        "Probe Completion Resume",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.activate_health_probes(job_id);
    pipeline.active_download_passes.insert(job_id);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.maybe_finish_download_pass(job_id);

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(!state.health_probing, "the probe should have been retired");
        assert!(matches!(state.status, JobStatus::Downloading));
    }
    assert!(
        pipeline.pending_completion_checks.contains(&job_id),
        "the checkpoint must not wait for the probe once the pass has drained"
    );

    // The abandoned round lands late; it must change nothing.
    pipeline.pending_completion_checks.clear();
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        probe_round: 0,
        total: 1,
        missed: 1,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(
        state.probe_projected_failed_bytes, 0,
        "a retired round must not project damage onto a settled ledger"
    );
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "a retired round must not re-drive the checkpoint"
    );
}

#[tokio::test]
async fn reconcile_job_progress_does_not_rewrite_status_from_lanes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30088);
    let spec = standalone_job_spec(
        "Restored Checking",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.health_probing = false;
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
async fn restore_job_reemits_open_download_finalization_and_drains_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30094);
    let spec = standalone_job_spec(
        "Restore Finalizing Download",
        &[("payload.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-finalizing-download");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .db
        .insert_job_events(&[
            crate::JobEvent {
                job_id: job_id.0,
                timestamp: 1,
                kind: "JobCreated".to_string(),
                message: "created".to_string(),
                file_id: None,
            },
            crate::JobEvent {
                job_id: job_id.0,
                timestamp: 2,
                kind: "DownloadFinished".to_string(),
                message: "download finished".to_string(),
                file_id: Some(
                    crate::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER.to_string(),
                ),
            },
        ])
        .unwrap();

    pipeline
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
            download_state: Some(crate::jobs::model::DownloadState::Complete),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert!(pipeline.jobs_finalizing_download.contains(&job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;

    let drained_events = drain_job_events(&mut events, job_id);
    assert!(
        drained_events.iter().any(|event| {
            matches!(
                event,
                PipelineEvent::DownloadFinished {
                    finalization_pending: true,
                    ..
                }
            )
        }),
        "events: {drained_events:?}"
    );
    let finish_idx = drained_events
        .iter()
        .position(|event| {
            matches!(
                event,
                PipelineEvent::DownloadFinished {
                    finalization_pending: true,
                    ..
                }
            )
        })
        .expect("restored download finished event");
    let drained_idx = drained_events
        .iter()
        .position(|event| matches!(event, PipelineEvent::DownloadPipelineDrained { .. }))
        .expect("download pipeline drained event");
    assert!(finish_idx < drained_idx, "events: {drained_events:?}");
    assert!(!pipeline.jobs_finalizing_download.contains(&job_id));
}

#[tokio::test]
async fn restore_paused_postprocessing_target_normalizes_to_downloading() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31206);
    let spec = standalone_job_spec("Restore paused", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-paused");
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
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: Some(84_000.0),
            paused_resume_status: Some(JobStatus::QueuedExtract),
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.queued_extract_at_epoch_ms),
        Some(84_000.0)
    );

    pipeline.resume_job_runtime(job_id).unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn active_rate_limit_does_not_prelease_multiple_bodies() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 4,
            medium_count: 2,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(31232);
    let files = (0..4)
        .map(|index| (format!("rate-limit-{index}.bin"), 512 * 1024))
        .collect::<Vec<_>>();
    let spec = standalone_job_spec("Rate Limited Lease", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.rate_limiter.set_rate(128 * 1024);
    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 3);
}

#[tokio::test]
async fn download_done_refunds_rate_limit_estimate_to_actual_raw_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(31233),
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.rate_limiter.set_rate(1_000);
    pipeline.rate_limiter.refund(1_000);
    pipeline.rate_limiter.consume(1_500);
    pipeline.rate_limit_reservations.insert(segment_id, 1_500);
    assert!(pipeline.rate_limiter.should_wait());

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from(vec![0; 500]))),
            attempts: vec![],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![],
            release_connection_slot: true,
        })
        .await;

    assert!(!pipeline.rate_limit_reservations.contains_key(&segment_id));
    assert!(!pipeline.rate_limiter.should_wait());
}

#[tokio::test]
async fn download_done_charges_rate_limit_for_raw_bytes_above_estimate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(31234),
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.rate_limiter.set_rate(1_000);
    pipeline.rate_limiter.refund(1_000);
    pipeline.rate_limiter.consume(500);
    pipeline.rate_limit_reservations.insert(segment_id, 500);
    assert!(!pipeline.rate_limiter.should_wait());

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from(vec![0; 1_600]))),
            attempts: vec![],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: vec![],
            release_connection_slot: true,
        })
        .await;

    assert!(!pipeline.rate_limit_reservations.contains_key(&segment_id));
    assert!(pipeline.rate_limiter.should_wait());
}

#[tokio::test]
async fn auto_pause_stalled_download_releases_blocking_runtime() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31232);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "stalled-job", temp_dir.path().join("stalled-job")),
    );
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline.bandwidth_cap.reserve(256);
    pipeline.bandwidth_reservations.insert(segment_id, 256);
    pipeline.rate_limit_reservations.insert(segment_id, 256);
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );

    pipeline.auto_pause_stalled_downloads();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.active_downloads, 0);
    assert!(!pipeline.active_download_passes.contains(&job_id));
    assert!(!pipeline.active_downloads_by_job.contains_key(&job_id));
    assert!(pipeline.bandwidth_reservations.is_empty());
    assert!(pipeline.rate_limit_reservations.is_empty());
}

#[tokio::test]
async fn auto_pause_ignores_stale_jobs_without_inflight_downloads() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31233);

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "idle-job", temp_dir.path().join("idle-job")),
    );
    pipeline.active_download_passes.insert(job_id);
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );

    pipeline.auto_pause_stalled_downloads();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.active_download_passes.contains(&job_id));
}

#[tokio::test]
async fn pause_allows_extracting_status_when_download_lane_is_active() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31241);

    let mut state = minimal_job_state(
        job_id,
        "extracting-job",
        temp_dir.path().join("extracting-job"),
    );
    state.post_state = crate::jobs::model::PostState::Extracting;
    state.refresh_legacy_status();
    pipeline.jobs.insert(job_id, state);

    pipeline.pause_job_runtime(job_id).unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Paused);
    assert_eq!(state.paused_resume_status, Some(JobStatus::Extracting));
    assert_eq!(
        state.paused_resume_download_state,
        Some(crate::jobs::model::DownloadState::Downloading)
    );
    assert_eq!(
        state.paused_resume_post_state,
        Some(crate::jobs::model::PostState::Extracting)
    );

    pipeline.resume_job_runtime(job_id).unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Extracting);
    assert_eq!(
        state.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(state.post_state, crate::jobs::model::PostState::Extracting);
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
async fn pause_allows_extracting_status_with_runtime_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31242);

    let mut state = minimal_job_state(
        job_id,
        "extracting-with-active-download",
        temp_dir.path().join("extracting-with-active-download"),
    );
    state.download_state = crate::jobs::model::DownloadState::Complete;
    state.post_state = crate::jobs::model::PostState::Extracting;
    state.refresh_legacy_status();
    pipeline.jobs.insert(job_id, state);
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline.pause_job_runtime(job_id).unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Paused);
    assert_eq!(state.paused_resume_status, Some(JobStatus::Extracting));
    assert_eq!(
        state.paused_resume_download_state,
        Some(crate::jobs::model::DownloadState::Complete)
    );
    assert_eq!(
        state.paused_resume_post_state,
        Some(crate::jobs::model::PostState::Extracting)
    );
}

#[tokio::test]
async fn pause_rejects_extracting_state_without_download_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31243);

    let mut state = minimal_job_state(
        job_id,
        "extracting-only-job",
        temp_dir.path().join("extracting-only-job"),
    );
    state.download_state = crate::jobs::model::DownloadState::Complete;
    state.post_state = crate::jobs::model::PostState::Extracting;
    state.refresh_legacy_status();
    pipeline.jobs.insert(job_id, state);

    let error = pipeline.pause_job_runtime(job_id).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
}

// ---- propagation delay ----

#[tokio::test]
async fn a_freshly_posted_job_defers_until_its_articles_have_propagated() {
    // A post is not on the server the instant it is made. Fetching it now
    // produces not-founds that look exactly like missing articles: they burn
    // retries, spend the article's server budget and mark servers unhealthy —
    // and on a par2-less job they can fail a download that would have worked
    // minutes later.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));
    let job_id = JobId(20200);
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec("Silver Horizon Fresh", Some(now_epoch_secs())),
    )
    .await;

    let hold = pipeline
        .propagation_hold_until(job_id)
        .expect("a post made now must be held");
    assert!(hold > Instant::now());

    // The call site itself is pinned by
    // `a_deferred_job_issues_nothing_until_it_is_eligible`, which drives
    // `dispatch_downloads` on a pipeline built with a connection to spend. This
    // test pins the decision function alone: at zero connections
    // `dispatch_downloads` never reaches per-job dispatch, so a status or queue
    // assertion here would hold whether or not the gate exists.

    // The run loop is told when to wake, rather than rediscovering this by
    // polling.
    let wake = pipeline
        .next_propagation_delay()
        .expect("a deferred job must expose its wakeup");
    assert!(wake <= Duration::from_secs(3600));
    assert!(wake > Duration::from_secs(3500));
}

#[tokio::test]
async fn a_deferred_job_issues_nothing_until_it_is_eligible() {
    // The gate's *call site*, driven end to end: `dispatch_downloads` picks the
    // job, reaches `try_dispatch_download_for_job`, and the first thing that
    // function does is ask whether the post has propagated.
    //
    // The fixture that makes this observable is one connection instead of zero.
    // Every other propagation test uses `new_direct_pipeline`, which is built
    // with `total_connections: 0`; the hot-dispatch loop is gated on
    // `active_download_connections < max`, so with no capacity it exits before
    // the per-job call and any assertion after it is vacuous. One connection is
    // the whole fixture — no fake pool and no production seam, and no ramp to
    // account for since dispatch reaches full configured capacity immediately.
    // The empty server list is fine: leasing, activation and the queue drain
    // are synchronous, and only the spawned fetch needs a server, exactly as
    // the surrounding hot-dispatch tests rely on.
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
    pipeline.propagation_delay_forced = Some(Duration::from_millis(150));

    let job_id = JobId(20209);
    // Dated one minute in the future: the gate saturates a future post to age
    // zero, so the first dispatch is held no matter how many wall-clock seconds
    // fixture setup takes on a loaded machine. A post dated exactly `now` flips
    // to eligible the moment one second ticks before `dispatch_downloads`,
    // because ages are whole epoch seconds and the delay here is only 150ms.
    // The second half is unaffected — the first consult caches an
    // `Instant`-based `ready_at`, and the sleep below outlives it.
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec(
            "Silver Horizon Deferred Dispatch",
            Some(now_epoch_secs() + 60),
        ),
    )
    .await;
    // `insert_active_job` admits at `Downloading`. A deferred job has never
    // dispatched a segment, so `Queued` is its real state — and staying there is
    // half of what the gate promises.
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Queued);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        1,
        "one file, one segment, waiting to be issued"
    );

    pipeline.dispatch_downloads();

    assert_eq!(
        pipeline.active_downloads, 0,
        "a deferred job must not put an article on the wire"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        1,
        "and its segment must still be queued, not leased"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Queued),
        "with the job still queued: a deferral is not a status"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    pipeline.dispatch_downloads();

    assert_eq!(
        pipeline.active_downloads, 1,
        "once the post has propagated the same pass dispatches it"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        0,
        "and the segment leaves the queue"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading),
        "the first dispatched segment is what moves the job to Downloading"
    );
}

#[tokio::test]
async fn the_deferral_boundary_is_posted_at_plus_the_delay() {
    // Pin both sides of the boundary with enough margin that a wall-clock
    // second ticking during fixture setup cannot change the expected side.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));
    let now = now_epoch_secs();

    let young = JobId(20201);
    insert_active_job(
        &mut pipeline,
        young,
        posted_job_spec("Silver Horizon Young", Some(now - 3540)),
    )
    .await;
    assert!(
        pipeline.propagation_hold_until(young).is_some(),
        "a post inside the delay is still deferred"
    );

    let old = JobId(20202);
    insert_active_job(
        &mut pipeline,
        old,
        posted_job_spec("Silver Horizon Old", Some(now - 3660)),
    )
    .await;
    assert!(
        pipeline.propagation_hold_until(old).is_none(),
        "a post older than the delay starts immediately"
    );
    assert!(
        !pipeline.propagation_ready_at.contains_key(&old),
        "and an eligible job leaves no deferral state behind"
    );
}

#[tokio::test]
async fn a_job_becomes_eligible_when_its_delay_elapses() {
    // The transition itself, on a real clock with a delay small enough to wait
    // for. Use the same future-date clamp as the dispatch regression above so
    // crossing an epoch-second boundary during setup cannot consume this
    // sub-second delay before the first consult caches its `Instant`.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_millis(120));
    let job_id = JobId(20203);
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec("Silver Horizon Soon", Some(now_epoch_secs() + 60)),
    )
    .await;
    assert!(
        pipeline.propagation_hold_until(job_id).is_some(),
        "held at first"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "and eligible once the delay has passed"
    );
    assert!(
        pipeline.next_propagation_delay().is_none(),
        "with nothing left for the run loop to wake for"
    );
}

#[tokio::test]
async fn a_job_with_no_parseable_dates_is_never_deferred() {
    // No date is not the same question as "posted just now". An NZB that
    // carries no usable dates has no anchor to defer against, so it starts
    // immediately — the same stance retention takes for the same reason.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));
    let job_id = JobId(20204);
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec("Silver Horizon Undated", None),
    )
    .await;

    assert!(pipeline.propagation_hold_until(job_id).is_none());
    assert!(pipeline.next_propagation_delay().is_none());
}

#[tokio::test]
async fn the_newest_date_in_the_nzb_is_the_anchor() {
    // Files with no date contribute nothing, and the anchor is the newest date
    // the NZB does carry — that is the article most likely to still be in
    // flight, and deferring to it is what makes the wait cover the whole post.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));
    let now = now_epoch_secs();
    let job_id = JobId(20205);
    let mut spec = standalone_job_spec(
        "Silver Horizon Mixed Dates",
        &[
            ("old.bin".to_string(), 512u32),
            ("new.bin".to_string(), 512),
        ],
    );
    spec.files[0].posted_at_epoch = Some(now - 86_400);
    spec.files[1].posted_at_epoch = None;
    insert_active_job(&mut pipeline, job_id, spec).await;
    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "a day-old anchor beside an undated file is eligible"
    );

    let fresh = JobId(20206);
    let mut spec = standalone_job_spec(
        "Silver Horizon Fresh Tail",
        &[
            ("old.bin".to_string(), 512u32),
            ("new.bin".to_string(), 512),
        ],
    );
    spec.files[0].posted_at_epoch = Some(now - 86_400);
    spec.files[1].posted_at_epoch = Some(now);
    insert_active_job(&mut pipeline, fresh, spec).await;
    assert!(
        pipeline.propagation_hold_until(fresh).is_some(),
        "one fresh file is enough to hold the job — its articles are the ones \
         still propagating"
    );
}

#[tokio::test]
async fn a_zero_delay_disables_deferral_entirely() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::ZERO);
    let job_id = JobId(20207);
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec("Silver Horizon Disabled", Some(now_epoch_secs())),
    )
    .await;

    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "WEAVER_PROPAGATION_DELAY_SECS=0 is the off switch"
    );
    assert!(pipeline.propagation_ready_at.is_empty());
}

#[tokio::test]
async fn deferral_survives_pause_resume_and_leaves_nothing_behind_on_delete() {
    // A deferral is invisible to the lifecycle: it holds no status, no queue
    // position and no lock, so pause and resume behave exactly as they always
    // did — and removing the job takes its deferral with it rather than
    // leaving a wakeup for a job that no longer exists.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.propagation_delay_forced = Some(Duration::from_secs(3600));
    let job_id = JobId(20208);
    insert_active_job(
        &mut pipeline,
        job_id,
        posted_job_spec("Silver Horizon Lifecycle", Some(now_epoch_secs())),
    )
    .await;
    assert!(pipeline.propagation_hold_until(job_id).is_some());

    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Paused);
    let _ = &job_id;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Paused),
        "pause works on a deferred job exactly as on any queued one"
    );
    pipeline.dispatch_downloads();
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Queued);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Queued),
        "and resume returns it to the ordinary queued state"
    );
    assert!(
        pipeline.propagation_hold_until(job_id).is_some(),
        "still deferred afterwards — the clock did not restart and did not stop"
    );

    pipeline.purge_terminal_job_runtime(job_id);
    assert!(
        !pipeline.propagation_ready_at.contains_key(&job_id),
        "a removed job must not leave a wakeup behind"
    );
    assert!(pipeline.next_propagation_delay().is_none());
}
