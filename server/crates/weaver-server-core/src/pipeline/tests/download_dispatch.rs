use super::*;

#[tokio::test]
async fn owned_download_lane_pool_respects_configured_connection_count_at_startup() {
    let low_temp_dir = tempfile::tempdir().unwrap();
    let (low_pipeline, _, _) = new_direct_pipeline_with_buffers(
        &low_temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        3,
    )
    .await;

    assert_eq!(low_pipeline.owned_download_lane_pool.worker_count(), 3);
    assert_eq!(low_pipeline.connection_ramp, 3);
    assert_eq!(low_pipeline.tuner.params().max_concurrent_downloads, 3);

    let high_temp_dir = tempfile::tempdir().unwrap();
    let (high_pipeline, _, _) = new_direct_pipeline_with_buffers(
        &high_temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        24,
    )
    .await;

    assert_eq!(high_pipeline.owned_download_lane_pool.worker_count(), 24);
    assert!(
        high_pipeline.connection_ramp < high_pipeline.owned_download_lane_pool.worker_count(),
        "high-connection pool must not be capped by the startup ramp"
    );
    assert_eq!(high_pipeline.tuner.params().max_concurrent_downloads, 24);
}

#[tokio::test]
async fn hot_lease_work_limit_scales_with_lane_throughput() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40142);
    let article_bytes: u32 = 768 * 1024;

    // No measured throughput yet: warmup batch, not the full 64 — the first
    // dispatch wave must not blind-lease several volumes on a slow link.
    assert_eq!(
        pipeline.hot_lease_work_limit(job_id, DownloadLaneMode::PipelineDepth4, article_bytes),
        16
    );

    // 120MB/s across 10 lanes -> 12MB/s per lane -> 2s runway is ~30 articles.
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 10);
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 240_000_000);
    assert_eq!(
        pipeline.hot_lease_work_limit(job_id, DownloadLaneMode::PipelineDepth4, article_bytes),
        30
    );

    // 7.5MB/s across 10 lanes -> under one article of runway -> clamp to the
    // lane's pipeline depth so slow links cycle back to the queue head.
    pipeline.hot_dispatch_throughput_window.clear();
    pipeline
        .hot_dispatch_throughput_window
        .record(Instant::now(), 15_000_000);
    assert_eq!(
        pipeline.hot_lease_work_limit(job_id, DownloadLaneMode::PipelineDepth4, article_bytes),
        4
    );
}

#[tokio::test]
async fn shutdown_drain_consumes_inflight_download_results() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JobId(42),
            file_index: 0,
        },
        segment_number: 1,
    };

    pipeline.active_downloads = 1;
    pipeline
        .active_downloads_by_job
        .insert(segment_id.file_id.job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    pipeline
        .download_done_tx
        .send(DownloadResult {
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::Transient,
                "shutdown test",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), pipeline.drain())
        .await
        .expect("shutdown drain should consume queued download result");

    assert_eq!(pipeline.active_downloads, 0);
    assert!(
        !pipeline
            .active_downloads_by_job
            .contains_key(&segment_id.file_id.job_id)
    );
    assert!(
        !pipeline
            .active_downloads_by_file
            .contains_key(&segment_id.file_id)
    );
}

#[tokio::test]
async fn transient_retry_backoff_does_not_fail_job_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20008);
    let spec = segmented_job_spec("Retry Backoff Guard", "retry.bin", &[128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::Transient,
                "connection reset by peer",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[test]
fn lane_acquire_failure_without_body_request_is_lane_unavailable() {
    let unavailable = DownloadFailure::from_lane_acquire_failure(None);
    assert_eq!(unavailable.kind, DownloadFailureKind::LaneUnavailable);

    let pool_miss =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::PoolExhausted));
    assert_eq!(pool_miss.kind, DownloadFailureKind::LaneUnavailable);

    let auth_failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::AuthenticationFailed,
    ));
    assert_eq!(auth_failure.kind, DownloadFailureKind::Auth);

    let setup_failure =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::NoSuchGroup));
    assert_eq!(setup_failure.kind, DownloadFailureKind::Permanent);
}

#[tokio::test]
async fn pool_capacity_failure_at_retry_limit_does_not_poison_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Pool Capacity Guard", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::CapacityUnavailable,
                "no connections available",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(pipeline.unavailable_promoted_recovery_segments.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("pool capacity miss should requeue without poisoning health")
        .work;
    assert_eq!(work.segment_id, segment_id);
    assert_eq!(work.retry_count, MAX_SEGMENT_RETRIES);
    assert_eq!(work.exclude_servers, vec![0]);
}

#[tokio::test]
async fn body_lane_unavailable_at_retry_limit_requeues_without_article_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20014);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Lane Acquire Guard", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: vec![0, 1],
            release_connection_slot: false,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(pipeline.unavailable_promoted_recovery_segments.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("pre-BODY lane miss should requeue without consuming article retry budget")
        .work;
    assert_eq!(work.segment_id, segment_id);
    assert_eq!(work.retry_count, MAX_SEGMENT_RETRIES + 1);
    assert_eq!(work.exclude_servers, vec![0, 1]);
}

#[tokio::test]
async fn server_quota_lane_failure_parks_until_retry_at_without_lane_spin() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20997);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Guard", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(7),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 9,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));
    assert_eq!(failure.kind, DownloadFailureKind::ServerQuota);
    assert!(
        failure
            .retry_after
            .is_some_and(|delay| delay > Duration::from_secs(20))
    );

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "server quota retry must wait for retry_at or an explicit policy change"
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake an indefinite quota waiter")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn quota_acquire_failure_requeues_smaller_tail_for_independent_selection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21002);
    let large_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let tail_segment = SegmentId {
        segment_number: 1,
        ..large_segment
    };
    let large_bytes = 5_000;
    let tail_bytes = 1_000;
    let spec = segmented_job_spec(
        "Heterogeneous Quota Lease",
        "heterogeneous.bin",
        &[large_bytes, tail_bytes],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 2;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 2);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let stable_id = weaver_nntp::transfer::StableServerId(17);
    let control = policy.transfer_registry().configure(
        stable_id,
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 10_000,
                generation: 17,
                retry_at: None,
            }),
        },
    );
    let reservation = control.try_reserve(5_000).unwrap();
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: "heterogeneous-fill.example.com".into(),
                    ..Default::default()
                },
                stable_id,
                transfer_control: Some(std::sync::Arc::clone(&control)),
                max_connections: 1,
                group: 0,
                backfill: false,
                retention_days: 0,
            },
            weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: "heterogeneous-backfill.example.com".into(),
                    ..Default::default()
                },
                stable_id: weaver_nntp::transfer::StableServerId(18),
                transfer_control: None,
                max_connections: 1,
                group: 0,
                backfill: true,
                retention_days: 0,
            },
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    pipeline.shared_state.set_server_transfer_policy(policy);

    let large_estimate = Pipeline::bandwidth_reservation_estimate(large_bytes);
    let blocked = pipeline
        .nntp
        .body_server_selection_with_estimate(&[], large_estimate)
        .await;
    assert!(blocked.eligible.is_empty());
    let failure =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::quota_blocked(
            blocked
                .quota_blocked
                .expect("large first work must be blocked"),
        )));
    let first_failure = crate::pipeline::download::lane_acquire_failure_for_work(&failure, 0);
    let tail_failure = crate::pipeline::download::lane_acquire_failure_for_work(&failure, 1);
    assert_eq!(first_failure.kind, DownloadFailureKind::ServerQuota);
    assert_eq!(tail_failure.kind, DownloadFailureKind::Unrequested);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: large_segment,
            data: Err(DownloadError::Fetch(first_failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 4,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: tail_segment,
            data: Err(DownloadError::Fetch(tail_failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 7,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    assert!(pipeline.server_quota_parked.contains(&large_segment));
    assert!(!pipeline.server_quota_parked.contains(&tail_segment));
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1),
        "only the tested large work should be quota-parked"
    );
    let tail_work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("untested tail must re-enter the download queue");
    assert_eq!(tail_work.segment_id, tail_segment);
    assert_eq!(tail_work.retry_count, 7);
    let tail_selection = pipeline
        .nntp
        .body_server_selection_with_estimate(
            &tail_work.exclude_servers,
            Pipeline::bandwidth_reservation_estimate(tail_work.byte_estimate),
        )
        .await;
    assert_eq!(tail_selection.eligible, vec![weaver_nntp::ServerId(0)]);
    assert!(
        !tail_selection.eligible.contains(&weaver_nntp::ServerId(1)),
        "reselecting the tail must not unlock backfill"
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake the parked large work")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    drop(reservation);
}

#[tokio::test]
async fn server_quota_reservation_refund_wakes_parked_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20998);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Refund", "refund.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let control = policy.transfer_registry().configure(
        weaver_nntp::transfer::StableServerId(8),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 10,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    pipeline.shared_state.set_server_transfer_policy(policy);
    let reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject while the estimate is reserved");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(pipeline.retry_rx.try_recv().is_err());

    drop(reservation);

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("reservation refund must wake quota-parked work")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn server_quota_source_failure_keeps_backfill_locked_and_fails_over_to_fill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20999);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Failover", "failover.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(9),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 11,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    let selection_server = |stable_id: u32,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("quota-selection-{stable_id}.example.com"),
            ..Default::default()
        },
        stable_id: weaver_nntp::transfer::StableServerId(stable_id),
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    let two_fills = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(11, false, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    pipeline.nntp = std::sync::Arc::new(two_fills);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("a quota-blocked source must immediately fail over")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    assert_eq!(
        retry.work.exclude_servers,
        Vec::<usize>::new(),
        "quota-blocked sources must not count as exhausted fills and unlock backfill"
    );
    assert!(!pipeline.server_quota_parked.contains(&segment_id));
    assert_ne!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );

    let fill_and_backfill = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(10, true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let quota_retry = fill_and_backfill
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert!(quota_retry.eligible.is_empty());
    assert!(quota_retry.quota_blocked.is_some());
    let missing_or_retention_retry = fill_and_backfill
        .body_server_selection_with_estimate(&[0], 1)
        .await;
    assert_eq!(
        missing_or_retention_retry.eligible,
        vec![weaver_nntp::ServerId(1)],
        "generic missing/retention exhaustion must still unlock backfill"
    );

    let two_fills = NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(9, false, Some(std::sync::Arc::clone(&control))),
            selection_server(11, false, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    });
    let fill_failover = two_fills
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert_eq!(fill_failover.eligible, vec![weaver_nntp::ServerId(1)]);

    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
}

#[tokio::test]
async fn server_quota_source_failure_parks_while_only_backfill_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21000);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Server Quota Backfill Lock", "backfill-lock.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(12),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 128,
                generation: 12,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let reservation = control.try_reserve(128).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota must reject the next reservation");
    let selection_server = |stable_id: u32,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("quota-park-{stable_id}.example.com"),
            ..Default::default()
        },
        stable_id: weaver_nntp::transfer::StableServerId(stable_id),
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(12, false, Some(std::sync::Arc::clone(&control))),
            selection_server(13, true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "quota must not unlock the backfill tier or spin an immediate retry"
    );
    assert!(pipeline.server_quota_parked.contains(&segment_id));
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::ServerQuota
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    let cancelled = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("job removal must wake the quota waiter")
        .expect("retry channel must stay open");
    pipeline.receive_retry_work(cancelled);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    drop(reservation);
}

#[tokio::test]
async fn other_fill_refund_wakes_manual_quota_park_without_unlocking_backfill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21001);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Manual Quota Refund", "manual-refund.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    let first_stable_id = weaver_nntp::transfer::StableServerId(14);
    let second_stable_id = weaver_nntp::transfer::StableServerId(15);
    let manual_quota = |generation| weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes: 128,
            generation,
            retry_at: None,
        }),
    };
    let first = policy
        .transfer_registry()
        .configure(first_stable_id, manual_quota(14));
    let second = policy
        .transfer_registry()
        .configure(second_stable_id, manual_quota(15));
    let first_reservation = first.try_reserve(128).unwrap();
    let second_reservation = second.try_reserve(128).unwrap();
    let selection_server = |stable_id: weaver_nntp::transfer::StableServerId,
                            backfill: bool,
                            transfer_control: Option<
        std::sync::Arc<weaver_nntp::transfer::ServerTransferControl>,
    >| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: format!("manual-quota-{}.example.com", stable_id.0),
            ..Default::default()
        },
        stable_id,
        transfer_control,
        max_connections: 1,
        group: 0,
        backfill,
        retention_days: 0,
    };
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![
            selection_server(first_stable_id, false, Some(std::sync::Arc::clone(&first))),
            selection_server(
                second_stable_id,
                false,
                Some(std::sync::Arc::clone(&second)),
            ),
            selection_server(weaver_nntp::transfer::StableServerId(16), true, None),
        ],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    pipeline.shared_state.set_server_transfer_policy(policy);

    let blocked = pipeline
        .nntp
        .body_server_selection_with_estimate(&[], 1)
        .await;
    assert!(blocked.eligible.is_empty());
    let rejection = blocked
        .quota_blocked
        .expect("both normal fills must be quota-blocked");
    assert!(rejection.retry_at.is_none());
    let selected_stable_id = rejection.stable_server_id;
    let selected_local_revision = rejection.capacity_revision;
    let (selected_reservation, other_reservation, other_server) =
        if selected_stable_id == first_stable_id {
            (
                first_reservation,
                second_reservation,
                weaver_nntp::ServerId(1),
            )
        } else {
            (
                second_reservation,
                first_reservation,
                weaver_nntp::ServerId(0),
            )
        };
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Err(DownloadError::Fetch(failure)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 1,
            exclude_servers: Vec::new(),
            release_connection_slot: false,
        })
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        pipeline.retry_rx.try_recv().is_err(),
        "manual quota has no timer and must remain parked before capacity changes"
    );
    assert!(pipeline.server_quota_parked.contains(&segment_id));

    drop(other_reservation);

    let retry = tokio::time::timeout(Duration::from_secs(1), pipeline.retry_rx.recv())
        .await
        .expect("a non-selected fill refund must wake parked work")
        .expect("retry channel must stay open");
    assert_eq!(retry.work.segment_id, segment_id);
    assert!(retry.work.exclude_servers.is_empty());
    let selected = if selected_stable_id == first_stable_id {
        &first
    } else {
        &second
    };
    assert_eq!(
        selected.quota_rejection_for(1).unwrap().capacity_revision,
        selected_local_revision,
        "the selected rejection remains locally current; only the other fill changed"
    );
    let resumed = pipeline
        .nntp
        .body_server_selection_with_estimate(&retry.work.exclude_servers, 1)
        .await;
    assert_eq!(resumed.eligible, vec![other_server]);
    assert!(
        !resumed.eligible.contains(&weaver_nntp::ServerId(2)),
        "quota capacity must not unlock the backfill tier"
    );

    pipeline.receive_retry_work(retry);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    drop(selected_reservation);
}

#[tokio::test]
async fn server_quota_lane_park_does_not_increment_error_counter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let before = pipeline
        .metrics
        .download_lane_parks_error_total
        .load(Ordering::Relaxed);

    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    pipeline.note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::ServerQuota);

    assert_eq!(
        pipeline
            .metrics
            .download_lane_parks_error_total
            .load(Ordering::Relaxed),
        before
    );
}

#[tokio::test]
async fn retention_excludes_derive_from_job_age_and_server_windows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40150);
    let spec = segmented_job_spec("Retention Derivation", "old.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());

    // Unknown post date: never retention-skipped.
    assert!(pipeline.job_retention_excludes(job_id).is_empty());

    // Older than server 0's five-day window: server 0 is excluded, and the
    // effective set merges with per-article failure excludes without dupes.
    age_job_days(&mut pipeline, job_id, 10);
    pipeline.clear_job_retention_excludes(job_id);
    assert_eq!(pipeline.job_retention_excludes(job_id).as_slice(), &[0]);
    assert_eq!(pipeline.effective_exclude_servers(job_id, &[1]), vec![1, 0]);
    assert_eq!(pipeline.effective_exclude_servers(job_id, &[0]), vec![0]);

    // Younger than every window: nothing excluded.
    age_job_days(&mut pipeline, job_id, 1);
    pipeline.clear_job_retention_excludes(job_id);
    assert!(pipeline.job_retention_excludes(job_id).is_empty());
}

#[tokio::test]
async fn article_not_found_exhaustion_counts_retention_excluded_servers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40151);
    let spec = segmented_job_spec("Retention Exhaustion", "aged.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    age_job_days(&mut pipeline, job_id, 10);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    // A miss on the deep server (index 1) plus the retention-excluded
    // short server (index 0) exhausts both servers: the article books
    // failed bytes instead of retrying forever.
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "article not found",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(1),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    // Exhaustion (not a retry) is the proof: the non-exhausted path
    // schedules a retry and never bumps articles_not_found. With no PAR2 in
    // the spec the booked failure also fails the job outright.
    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "retention-excluded server must count toward exhaustion"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn fully_retention_excluded_job_books_missing_instead_of_requeueing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40152);
    let spec = segmented_job_spec("Beyond All Retention", "ancient.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[5, 5]));
    age_job_days(&mut pipeline, job_id, 10);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    // Every server is retention-excluded, so the lane path never fetches —
    // the failure surfaces as LaneUnavailable. It must be booked as a
    // missing article, not requeued without budget forever.
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::LaneUnavailable,
                "failed to acquire BODY lane",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "an unserveable article must be booked missing"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn traced_article_not_found_retries_other_servers_without_retry_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let spec = segmented_job_spec("Source Not Found", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "article not found on source server",
            )),
            attempts: vec![weaver_nntp::client::FetchAttemptTrace {
                server_idx: 0,
                remote_ip: None,
                elapsed: Duration::from_millis(5),
                outcome: weaver_nntp::client::FetchAttemptOutcome::NotFound,
                error: Some("article not found".to_string()),
            }],
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert!(pipeline.pending_completion_checks.is_empty());

    tokio::time::sleep(Duration::from_millis(10)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("source miss should requeue against another server")
        .work;
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(work.retry_count, 0);
}

#[tokio::test]
async fn recovery_article_not_found_does_not_mark_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20012);
    let spec = standalone_with_par2_job_spec("Recovery Miss", 128, 64);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ArticleNotFound,
                "article not found",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::Recovery,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn exhausted_incomplete_download_fails_instead_of_hanging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "stalled.bin";
    let first_segment_size = 6400u32;
    let second_segment_size = 128u32;
    let total_size = (first_segment_size + second_segment_size) as u64;
    let spec = segmented_job_spec(
        "Incomplete Exhausted",
        filename,
        &[first_segment_size, second_segment_size],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Ok(DownloadPayload::Raw(encode_article_part(
                filename,
                &vec![1u8; first_segment_size as usize],
                1,
                2,
                1,
                total_size,
            ))),
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

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .complete_data_file_count(),
        0
    );

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::Transient,
                "connection reset by peer",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued_job).await;
    }

    let status = job_status_for_assert(&pipeline, job_id).expect("job should be archived");
    assert!(
        matches!(status, JobStatus::Failed { .. }),
        "status: {status:?}"
    );
}

#[tokio::test]
async fn download_pass_finishes_when_only_optional_recovery_queue_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Optional Recovery Only", "payload.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Downloading;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("recovery-0@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 0);

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));

    pipeline.maybe_finish_download_pass(job_id);

    assert!(!pipeline.active_download_passes.contains(&job_id));
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().recovery_queue.len(),
        1,
        "optional recovery files should stay parked until promoted"
    );
}

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
        groups: vec!["alt.binaries.test".to_string()],
        priority: FileRole::Standalone.download_priority(),
        byte_estimate: segment_bytes,
        retry_count: 0,
        is_recovery: false,
        exclude_servers: vec![],
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
    pipeline.connection_ramp = 1;
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
    pipeline.connection_ramp = 1;
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
        groups: vec!["alt.binaries.test".to_string()],
        priority: FileRole::RarVolume {
            volume_number: complete_files,
        }
        .download_priority(),
        byte_estimate: file_bytes,
        retry_count: 0,
        is_recovery: false,
        exclude_servers: vec![],
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
    pipeline.connection_ramp = 1;

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
    pipeline.connection_ramp = 1;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 99,
        },
        decoded_size: 4096,
        data: DecodedChunk::from(vec![7u8; 4096]),
        part_crc: weaver_par2::checksum::crc32(&vec![7u8; 4096]),
        part_crc_verified: true,
        yenc_name: "queued.bin".to_string(),
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
    pipeline.connection_ramp = 2;
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
    pipeline.connection_ramp = 2;

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
    pipeline.connection_ramp = 1;

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
async fn dispatch_downloads_bounded_same_band_shares_capacity() {
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
    pipeline.connection_ramp = 8;

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
        Some(&6)
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
    assert_eq!(peer_lane_total, 2);
    assert_eq!(
        job_ids[1..]
            .iter()
            .filter(|job_id| pipeline
                .active_download_connections_by_job
                .get(job_id)
                .copied()
                .unwrap_or(0)
                > 0)
            .count(),
        2
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .bounded_lent_connections(),
        2
    );
    assert_eq!(
        pipeline
            .metrics
            .hot_dispatch_spillover_allowed_bounded_same_band_total
            .load(Ordering::Relaxed),
        2
    );
}

#[tokio::test]
async fn dispatch_downloads_bounded_same_band_skips_blocked_peer_and_tries_next() {
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
    pipeline.connection_ramp = 8;

    let hot_job_id = JobId(21404);
    let blocked_peer_id = JobId(21405);
    let good_peer_a = JobId(21406);
    let good_peer_b = JobId(21407);

    let hot_files = many_standalone_files("bounded-skip-hot", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        standalone_job_spec("Bounded Skip Hot", &hot_files),
    )
    .await;

    let durable_lead_limit = Pipeline::restart_durable_lead_limit_bytes();
    let segment_bytes = 1024 * 1024u32;
    let segment_count = (durable_lead_limit / u64::from(segment_bytes) + 2) as u32;
    let blocked_segments = (0..segment_count)
        .map(|segment| {
            segment_spec! {
                number: segment,
                bytes: segment_bytes,
                message_id: format!("bounded-skip-blocked-{segment}@example.com"),
            }
        })
        .collect::<Vec<_>>();
    insert_active_job(
        &mut pipeline,
        blocked_peer_id,
        JobSpec {
            name: "Bounded Skip Blocked".to_string(),
            password: None,
            total_bytes: u64::from(segment_count) * u64::from(segment_bytes),
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: "blocked.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: blocked_segments,
            }],
        },
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&blocked_peer_id).unwrap();
        state.downloaded_bytes = durable_lead_limit;
        state.restored_download_floor_bytes = 1;
    }
    pipeline.active_decodes_by_job.insert(blocked_peer_id, 1);

    for (job_id, name) in [
        (good_peer_a, "Bounded Skip Good A"),
        (good_peer_b, "Bounded Skip Good B"),
    ] {
        let files = many_standalone_files(name, 600);
        insert_active_job(&mut pipeline, job_id, standalone_job_spec(name, &files)).await;
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.hot_dispatch_job, Some(hot_job_id));
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&hot_job_id),
        Some(&6)
    );
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&blocked_peer_id),
        None
    );
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&good_peer_a),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&good_peer_b),
        Some(&1)
    );
    assert_eq!(
        pipeline
            .hot_dispatch_spillover_loans
            .bounded_lent_connections(),
        2
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
    pipeline.connection_ramp = 8;

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
    // warmup batch rather than the full hot runway.
    assert_eq!(
        pipeline.active_downloads_by_job.get(&job_id),
        Some(&(8 * TEST_HOT_LEASE_WARMUP_WORK_LIMIT))
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
    pipeline.connection_ramp = 8;

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
            .bounded_lent_connections(),
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
    pipeline.connection_ramp = 8;

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
            .bounded_lent_connections(),
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
    let hot_job_id = JobId(21432);

    pipeline.hot_share_yield_signal.request(hot_job_id);
    pipeline.global_paused = true;
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));
    pipeline.global_paused = false;

    pipeline.hot_share_yield_signal.request(hot_job_id);
    pipeline.rate_limiter.set_rate(1_000);
    pipeline.rate_limiter.refund(1_000);
    pipeline.rate_limiter.consume(1_500);
    assert!(pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));

    pipeline.hot_share_yield_signal.request(hot_job_id);
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
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));

    pipeline.apply_bandwidth_cap_policy(None).unwrap();
    pipeline.hot_share_yield_signal.request(hot_job_id);
    pipeline.decode_backlog_budget_bytes = 1_000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(1_000, Ordering::Relaxed);
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));

    pipeline.hot_share_yield_signal.request(hot_job_id);
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);
    pipeline.download_pressure_soft_dispatch_after = Some(Instant::now() + Duration::from_secs(5));
    pipeline.dispatch_downloads();
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));
}

#[tokio::test]
async fn hot_share_yield_signal_clears_when_refill_gates_disable_bounded_share() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21433);

    pipeline.hot_share_yield_signal.request(job_id);
    pipeline.global_paused = true;
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: FileRole::Standalone.download_priority(),
            is_recovery: false,
            groups: vec!["alt.binaries.test".to_string()],
            exclude_servers: Vec::new(),
        },
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert!(!pipeline.hot_share_yield_signal.is_requested_for(job_id));
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
    pipeline.connection_ramp = 2;

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
    assert!(
        pipeline
            .metrics
            .hot_dispatch_spillover_blocked_warmup_total
            .load(Ordering::Relaxed)
            >= 1
    );

    let warmed = Instant::now();
    pipeline.hot_dispatch_started_at = Some(warmed - Duration::from_secs(5));
    pipeline.hot_dispatch_underfill_since = None;
    pipeline.hot_dispatch_successes = 0;

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(
        pipeline
            .jobs
            .get(&later_job_id)
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert!(pipeline.hot_dispatch_underfill_since.is_some());
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Exclusive);

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
    pipeline.connection_ramp = 1;

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
    // the warmup batch; the full 64-article runway resumes once the
    // throughput window is warm (see hot_lease_work_limit_scales_with_lane_throughput).
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
async fn hot_lane_refill_yields_when_bounded_share_unmet() {
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
    pipeline.connection_ramp = 8;

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
        job_id: hot_job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::HotShareYield);
    assert!(pipeline.hot_share_yield_signal.is_requested_for(hot_job_id));
}

#[tokio::test]
async fn bounded_same_band_refill_survives_hot_queued_primary() {
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
    pipeline.connection_ramp = 8;

    let hot_job_id = JobId(21450);
    let peer_job_id = JobId(21451);
    let hot_files = many_standalone_files("bounded-refill-hot", 600);
    let peer_files = many_standalone_files("bounded-refill-peer", 600);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Bounded Refill Hot", &hot_files),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer_job_id,
        with_priority(
            standalone_job_spec("Bounded Refill Peer", &peer_files),
            "NORMAL",
        ),
    )
    .await;

    let now = Instant::now();
    pipeline.hot_dispatch_job = Some(hot_job_id);
    pipeline.hot_dispatch_started_at = Some(now - Duration::from_secs(5));
    pipeline.hot_dispatch_mode = DispatchShareMode::Shared;
    pipeline.active_download_connections = 7;
    pipeline
        .active_download_connections_by_job
        .insert(hot_job_id, 6);
    pipeline
        .active_download_connections_by_job
        .insert(peer_job_id, 1);
    pipeline.active_downloads_by_job.insert(
        hot_job_id,
        6 * TEST_HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT,
    );
    pipeline
        .active_downloads_by_job
        .insert(peer_job_id, TEST_HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT);
    pipeline.hot_dispatch_spillover_loans.start_or_extend(
        peer_job_id,
        now,
        10_000,
        SpilloverLoanKind::BoundedSameBand,
    );

    let refill_compatibility = {
        let state = pipeline.jobs.get_mut(&peer_job_id).unwrap();
        let sample = state.download_queue.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&sample);
        state.download_queue.push(sample);
        compatibility
    };
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        job_id: peer_job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: Some(SpilloverLoanKind::BoundedSameBand),
        compatibility: refill_compatibility,
        response_tx,
    });

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("bounded same-band lane should refill while in budget");
    assert_eq!(lease.job_id, peer_job_id);
    assert_eq!(response.park_reason, LaneParkReason::NoWork);
    assert_eq!(
        lease.spillover_loan_kind,
        Some(SpilloverLoanKind::BoundedSameBand)
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_granted_total
            .load(Ordering::Relaxed),
        1
    );
}

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
    pipeline.connection_ramp = 2;

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
    pipeline.hot_dispatch_successes = 24;
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
        job_id: spillover_job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
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

#[tokio::test]
async fn dispatch_downloads_blocks_spillover_when_recent_expansion_helped() {
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
    pipeline.connection_ramp = 2;

    let hot_job_id = JobId(20027);
    let spillover_job_id = JobId(20028);

    insert_active_job(
        &mut pipeline,
        hot_job_id,
        with_priority(
            standalone_job_spec("Hot Expansion Helped", &[("hot.bin".to_string(), 512u32)]),
            "NORMAL",
        ),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        spillover_job_id,
        with_priority(
            standalone_job_spec(
                "Blocked Spillover",
                &[
                    ("spillover-0.bin".to_string(), 512u32),
                    ("spillover-1.bin".to_string(), 512u32),
                ],
            ),
            "NORMAL",
        ),
    )
    .await;

    pipeline.dispatch_downloads();
    let now = Instant::now();
    pipeline.hot_dispatch_started_at = Some(now - Duration::from_secs(5));
    pipeline.hot_dispatch_successes = 24;
    pipeline.hot_dispatch_underfill_since = Some(now - Duration::from_secs(2));
    pipeline
        .hot_dispatch_expansion_window
        .events
        .push_back(HotExpansionEvent {
            at: now - Duration::from_secs(3),
            kind: HotExpansionKind::LaneStart,
            before_bps: 10_000,
            after_bps: Some(10_600),
        });

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_download_connections, 1);
    assert_eq!(pipeline.hot_dispatch_mode, DispatchShareMode::Exclusive);
    assert_eq!(
        pipeline.hot_dispatch_last_spillover_decision,
        SpilloverDecision::BlockedRecentExpansionHelped
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&spillover_job_id)
            .unwrap()
            .download_queue
            .len(),
        2
    );
}

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
    pipeline.connection_ramp = 2;

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
    pipeline.hot_dispatch_successes = 24;
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
        job_id: spillover_job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
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

#[tokio::test]
async fn release_download_result_updates_hot_job_successes_before_heavy_processing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20022);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.hot_dispatch_job = Some(job_id);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(2));
    pipeline.hot_dispatch_successes = 23;
    pipeline.active_downloads = 1;
    pipeline.active_download_connections = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    pipeline.release_download_result(&DownloadResult {
        segment_id,
        data: Ok(DownloadPayload::Raw(Bytes::from_static(b"article"))),
        attempts: vec![],
        lane_observation: None,
        source_server_idx: None,
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: vec![],
        release_connection_slot: true,
    });

    assert_eq!(pipeline.hot_dispatch_successes, 24);
    assert_eq!(
        pipeline.metrics.hot_dispatch_job_id.load(Ordering::Relaxed),
        job_id.0
    );
    assert_eq!(
        pipeline
            .metrics
            .hot_dispatch_warmup_complete
            .load(Ordering::Relaxed),
        1
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

    pipeline
        .process_released_download_done(DownloadResult {
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
                segment_id,
                data: Ok(DownloadPayload::Raw(Bytes::from_static(b"owned"))),
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
            ack,
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
    assert!(ack_rx.try_recv().is_ok());
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
            ack,
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

#[test]
fn bounded_same_band_does_not_reclaim_on_hot_only_speed_drop() {
    let now = Instant::now();
    let mut loans = SpilloverLoanBook::default();
    let peer_job = JobId(21031);

    loans.start_or_extend(peer_job, now, 10_000, SpilloverLoanKind::BoundedSameBand);

    assert_eq!(loans.active_lent_connections(), 1);
    assert_eq!(loans.bounded_lent_connections(), 1);
    assert_eq!(loans.speed_snapshot(), (0, 0, 1));
    assert!(!loans.update_speed_harm(now + Duration::from_secs(2), 1, 7));
    assert!(!loans.reclaim_pending_for(peer_job));
    assert_eq!(loans.bounded_lent_connections(), 1);
}

#[test]
fn spillover_loan_book_releases_mixed_kinds_independently() {
    let now = Instant::now();
    let mut loans = SpilloverLoanBook::default();
    let peer_job = JobId(21032);

    loans.start_or_extend(peer_job, now, 10_000, SpilloverLoanKind::MeasuredUnderfill);
    loans.start_or_extend(peer_job, now, 10_000, SpilloverLoanKind::BoundedSameBand);

    assert_eq!(loans.active_lent_connections(), 2);
    assert_eq!(loans.bounded_lent_connections(), 1);
    assert_eq!(loans.active_loan_count(), 1);

    assert!(loans.update_speed_harm(now + Duration::from_secs(2), 8_000, 7));
    assert!(loans.reclaim_pending_for(peer_job));

    loans.release_one(peer_job, SpilloverLoanKind::BoundedSameBand);
    assert_eq!(loans.active_lent_connections(), 1);
    assert_eq!(loans.bounded_lent_connections(), 0);
    assert!(loans.reclaim_pending_for(peer_job));

    loans.release_one(peer_job, SpilloverLoanKind::MeasuredUnderfill);
    assert_eq!(loans.active_lent_connections(), 0);
    assert_eq!(loans.active_loan_count(), 0);
    assert!(!loans.reclaim_pending_for(peer_job));
}

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
    pipeline.hot_dispatch_successes = 23;
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .active_downloads_by_file
        .insert(segment_id.file_id, 1);

    pipeline.release_download_result(&DownloadResult {
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

    assert_eq!(pipeline.hot_dispatch_successes, 23);
    assert_eq!(
        pipeline
            .metrics
            .hot_dispatch_warmup_complete
            .load(Ordering::Relaxed),
        0
    );
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
        job_id: JobId(21004),
        server_idx: old_key.server_idx,
        remote_ip: old_key.ip,
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: 3,
            is_recovery: false,
            groups: vec!["alt.binaries.test".to_string()],
            exclude_servers: Vec::new(),
        },
        response_tx,
    });

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::IpReplacementRetired);
}

#[tokio::test]
async fn ip_replacement_trial_starts_when_recovery_reserve_leaves_normal_capacity_full() {
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
    pipeline.connection_ramp = configured_download_capacity;
    pipeline.ip_replacement_trial_extra_connections = 1;

    let mut snapshot = pipeline.metrics.raw_snapshot();
    snapshot.current_download_speed = 200 * 1024 * 1024;
    assert!(pipeline.tuner.adjust(&snapshot));
    let params = pipeline.tuner.params();
    let normal_download_capacity = configured_download_capacity
        .saturating_sub(params.recovery_slots.min(configured_download_capacity));
    assert!(normal_download_capacity > 0);
    assert!(normal_download_capacity < configured_download_capacity);

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
    pipeline.connection_ramp = 2;

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
    assert_eq!(pipeline.active_download_connections, 1);
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
    pipeline.connection_ramp = 2;
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
    pipeline.connection_ramp = 2;
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
    pipeline.connection_ramp = 1;

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

    // 65 queued articles minus the 16-article warmup first-wave lease.
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
async fn list_jobs_projects_downloading_while_extracting_with_remaining_download_work() {
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
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(info.post_state, crate::jobs::model::PostState::Extracting);
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
    pipeline.connection_ramp = 1;

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
    pipeline.connection_ramp = 1;
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
            segment_id,
            data: Ok(DownloadPayload::Decoded(DecodeResult {
                segment_id,
                raw_size,
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
                file_offset: 0,
                decoded_size: payload.len() as u32,
                crc_valid: true,
                part_crc_verified: false,
                part_crc: weaver_par2::checksum::crc32(&payload),
                expected_file_crc: None,
                data: DecodedChunk::from(payload.clone()),
                yenc_name: filename.to_string(),
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
    let provenance = pipeline
        .unverified_segments
        .get(&segment_id)
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

#[tokio::test]
async fn probe_completion_requeues_completion_check_if_downloads_finished_while_checking() {
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
    assert!(pipeline.pending_completion_checks.is_empty());

    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        total: 1,
        missed: 0,
        done: true,
        inconclusive: false,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert!(pipeline.pending_completion_checks.contains(&job_id));
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
