// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `download_dispatch` tests, part of a mechanical split of the original file.

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
    assert_eq!(high_pipeline.tuner.params().max_concurrent_downloads, 24);
}

// Recovery rides the same owned lanes as ordinary work.
//
// Recovery used to be pushed onto the async pool and pinned to sequential
// mode: the work a job is *waiting on* to finish paid a cold dial and gave
// back the round trip pipelining exists to hide. A recovery lease is now an
// ordinary lease as far as lane selection and depth are concerned: depth is a
// property of the server the lane sits on, and nothing reads the class.
#[tokio::test]
async fn a_recovery_lease_takes_an_owned_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40143);
    // A plaintext server: an owned lane is no longer a TLS-only arrangement.
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "plain.example.invalid".to_string(),
                port: 119,
                tls: false,
                ..Default::default()
            },
            max_connections: 2,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    }));
    let work = DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 1,
            },
            segment_number: 1,
        },
        message_id: MessageId::new("recovery-lane@example.invalid"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 1024,
        retry_count: 0,
        is_recovery: true,
        completion_critical: true,
        exclude_servers: Vec::new(),
        avoid_server: None,
    };
    let lease = DownloadBatchLease {
        lane_id: 0,
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: work.completion_critical,
        effective_exclude_servers: Vec::new(),
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };

    assert!(
        pipeline
            .nntp
            .has_blocking_body_lane_candidate(&lease.dial_exclude_servers),
        "a recovery lease must be eligible for a cached owned lane"
    );
}

// Dispatching recovery must never tear the lane fleet down.
//
// Recovery once came off a separate engine and had to prise a connection
// permit away from the idle owned lanes to get one; a lease that resets the
// fleet throws away every warm connection the job is about to need.
#[tokio::test]
async fn a_recovery_lease_keeps_the_owned_lane_fleet_intact() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let work = DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(40141),
                file_index: 1,
            },
            segment_number: 1,
        },
        message_id: MessageId::new("recovery-handoff@example.invalid"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 0,
        byte_estimate: 1024,
        retry_count: 0,
        is_recovery: true,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    };
    let lease = DownloadBatchLease {
        lane_id: 0,
        job_id: work.segment_id.file_id.job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: work.completion_critical,
        effective_exclude_servers: Vec::new(),
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works: vec![work],
    };

    assert_eq!(pipeline.owned_download_lane_pool.reset_calls(), 0);
    pipeline.spawn_download_batch(lease);
    assert_eq!(
        pipeline.owned_download_lane_pool.reset_calls(),
        0,
        "a recovery lease is dispatched onto the owned lanes without resetting them"
    );
}

// One refill carries a lane two ring turns, and no more.
//
// The worker asks again once its pending tail falls to `2 * depth + 1`, so a
// handout one deeper than that keeps the cadence at one refill per turn. A
// wider handout would pre-lease articles the job's other lanes could be
// fetching now, and leave the last lane draining a long tail alone.
#[tokio::test]
async fn a_refill_handout_is_two_ring_turns_deep() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;

    assert_eq!(
        pipeline.download_refill_want(DownloadLaneMode::Sequential),
        4
    );
    assert_eq!(
        pipeline.download_refill_want(DownloadLaneMode::Pipelined { depth: 4 }),
        10
    );
    assert_eq!(
        pipeline.download_refill_want(DownloadLaneMode::Pipelined { depth: 8 }),
        18
    );

    // A limited link activates its reservations after the lease is finalized,
    // so every refill has to see the balance the last one left behind.
    pipeline.rate_limiter.set_rate(1);
    assert!(pipeline.rate_limiter.is_limited());
    assert_eq!(
        pipeline.download_refill_want(DownloadLaneMode::Pipelined { depth: 8 }),
        1
    );
}

// A handout is exactly what the lane asked for while the queue can fill it,
// and never more — the queue's remainder is left for the other lanes.
#[tokio::test]
async fn a_handout_is_bounded_by_the_want_not_by_the_queue() {
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

    let job_id = JobId(40143);
    let files = many_standalone_files("lane-tail", 33);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Lane Tail", &files),
    )
    .await;
    assert_eq!(pipeline.current_hot_job(), Some(job_id));

    let want = pipeline.download_refill_want(DownloadLaneMode::Sequential);
    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("the hot job can serve this server");
    assert_eq!(lease.works.len(), want);
    assert_eq!(
        pipeline.jobs[&job_id].download_queue.len(),
        33 - want,
        "the rest of the tail stays queued for the lanes behind this one"
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

    hold_on_test_lane(&mut pipeline, &[segment_id]);

    pipeline
        .download_done_tx
        .send(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
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

    pipeline.drain().await;

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

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
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
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should remain in the batched infrastructure queue");
    assert_eq!(retry.retry_count, 0);

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: retry.segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id: retry.segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "connection reset by peer",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: retry.exclude_servers,
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
}

// A transport-failure retry must point away from the server that just
// failed when an alternative exists (`avoid_server`), without entering the
// 430-exhaustion ledger — otherwise sustained stochastic stall on a
// healthy-looking primary can burn every retry on the same server while a
// clean backup idles, and one transient timeout could later help declare an
// article missing.
#[tokio::test]
async fn transport_failure_retry_rotates_off_the_failed_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(20009);
    let spec = segmented_job_spec("Transport Rotation", "rotation.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(retry.avoid_server, Some(0));
    assert!(
        retry.exclude_servers.is_empty(),
        "rotation must not enter the article-not-found exhaustion ledger"
    );
    assert_eq!(retry.retry_count, 0);
}

// With a single configured server there is nowhere to rotate to: the retry
// must stay eligible for that server instead of dead-ending.
#[tokio::test]
async fn transport_failure_retry_keeps_single_server_eligible() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Transport Rotation Solo", "rotation-solo.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(retry.avoid_server, None);
    assert!(retry.exclude_servers.is_empty());
}

// A backfill server is not a rotation target: the avoid hint joins the
// lease's effective excludes, and excluding the whole fill tier would
// unlock backfill (`fill_servers_exhausted`) for work the fill server can
// serve on the next attempt — backfill is reserved for missing articles.
#[tokio::test]
async fn transport_failure_retry_does_not_rotate_toward_backfill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let servers = vec![
        weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "fill.example.com".into(),
                ..Default::default()
            },
            max_connections: 2,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        },
        weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "backfill.example.com".into(),
                ..Default::default()
            },
            max_connections: 2,
            backfill: true,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        },
    ];
    pipeline.nntp = std::sync::Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers,
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    let job_id = JobId(20011);
    let spec = segmented_job_spec("Transport Rotation Backfill", "rotation-bf.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "article fetch soft timeout (15s)",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(
        retry.avoid_server, None,
        "rotation must not manufacture fill-tier exhaustion"
    );
}

// Fails `segment_id` on an established connection and returns the delay its
// retry was parked for plus the retry count the requeued work carries.
async fn fail_segment_on_transport(
    pipeline: &mut Pipeline,
    segment_id: SegmentId,
    retry_count: u32,
) -> (Duration, u32) {
    let job_id = segment_id.file_id.job_id;
    hold_on_test_lane(pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);
    let before = tokio::time::Instant::now();
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::from_nntp(
                weaver_nntp::NntpError::ServerDisconnectedMidBody,
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;
    let deadline = pipeline
        .infrastructure_retries
        .iter_with_deadlines()
        .find_map(|(deadline, retry)| (retry.work.segment_id == segment_id).then_some(deadline))
        .expect("transport failure parks a retry")
        .expect("the parked retry is timed");
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    (
        deadline.saturating_duration_since(before),
        retry.retry_count,
    )
}

// Paused time keeps the clock still while the fault is handled, so the
// parked delay is exactly the one the scheduler chose.
#[tokio::test(start_paused = true)]
async fn a_segment_that_keeps_breaking_connections_is_held_back_then_pays_for_retries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Poison article", "poison.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();

    let (delay, retry_count) = fail_segment_on_transport(&mut pipeline, segment_id, 0).await;
    assert!(
        delay < Duration::from_secs(5),
        "a first fault retries promptly"
    );
    assert_eq!(retry_count, 0);

    // The repeat is held past the server recovery backoff, so the recovery
    // probe carries some other segment.
    let (delay, retry_count) = fail_segment_on_transport(&mut pipeline, segment_id, 0).await;
    assert!(delay > Duration::from_secs(60), "held for {delay:?}");
    assert_eq!(retry_count, 0);

    // Other articles downloading meanwhile makes the fault this article's.
    pipeline
        .metrics
        .segments_downloaded
        .fetch_add(1, Ordering::Relaxed);
    let (_, retry_count) = fail_segment_on_transport(&mut pipeline, segment_id, 0).await;
    assert_eq!(retry_count, 1, "the article now spends its retry budget");
}

#[tokio::test]
async fn an_outage_never_spends_article_retry_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20032);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Outage", "outage.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();

    for _ in 0..6 {
        let (_, retry_count) = fail_segment_on_transport(&mut pipeline, segment_id, 0).await;
        assert_eq!(retry_count, 0, "nothing downloads in an outage");
    }
}

#[test]
fn lane_acquire_failure_preserves_retry_semantics() {
    for error in [
        weaver_nntp::NntpError::SoftTimeout(15),
        weaver_nntp::NntpError::TruncatedMultilineBody,
        weaver_nntp::NntpError::MalformedMultilineTerminator,
    ] {
        let failure = DownloadFailure::from_nntp(error);
        assert_eq!(failure.kind, DownloadFailureKind::EstablishedTransport);
        assert!(failure.kind.preserves_article_retry_budget());
        assert!(failure.kind.infrastructure_wait_reason().is_some());
    }
    // A session that expired part-way through a batch costs the connection,
    // not the server: it keeps the article's retry budget and is never booked
    // as an auth failure, which would disable the server for the auth backoff
    // and unlock the backfill tier behind it.
    let expired = DownloadFailure::from_nntp(weaver_nntp::NntpError::SessionExpired);
    assert_eq!(expired.kind, DownloadFailureKind::EstablishedTransport);
    assert!(expired.kind.preserves_article_retry_budget());

    let unavailable = DownloadFailure::from_lane_acquire_failure(None);
    assert_eq!(unavailable.kind, DownloadFailureKind::LaneUnavailable);

    let pool_miss =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::PoolExhausted));
    assert_eq!(pool_miss.kind, DownloadFailureKind::CapacityUnavailable);

    let timeout =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::SoftTimeout(15)));
    assert_eq!(timeout.kind, DownloadFailureKind::ConnectionEstablishment);

    let io_failure = DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::Io(
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
    )));
    assert_eq!(
        io_failure.kind,
        DownloadFailureKind::ConnectionEstablishment
    );

    let auth_failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::AuthenticationFailed,
    ));
    assert_eq!(auth_failure.kind, DownloadFailureKind::Auth);

    let setup_failure =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::NoSuchGroup));
    assert_eq!(setup_failure.kind, DownloadFailureKind::ContentOrProtocol);
}

#[tokio::test]
async fn group_discovery_at_retry_limit_preserves_the_article_for_a_grouped_retry() {
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
    let spec = segmented_job_spec("Group discovery", "group.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            runtime_generation: 0,
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            segment_id,
            data: Err(DownloadError::from_nntp(
                weaver_nntp::NntpError::NoGroupSelected,
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.failed_bytes, 0);
    assert_eq!(state.status, JobStatus::Downloading);
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
    assert!(retry.exclude_servers.is_empty());
    assert_eq!(
        retry.avoid_server, None,
        "the same provider can select the learned group"
    );

    let setup =
        DownloadFailure::from_lane_acquire_failure(Some(&weaver_nntp::NntpError::NoGroupSelected));
    assert!(setup.kind.preserves_article_retry_budget());
}

#[test]
fn infrastructure_failures_preserve_article_retry_budget() {
    for kind in [
        DownloadFailureKind::CapacityUnavailable,
        DownloadFailureKind::ConnectionEstablishment,
        DownloadFailureKind::EstablishedTransport,
        DownloadFailureKind::Auth,
        DownloadFailureKind::ServerQuota,
        DownloadFailureKind::LaneUnavailable,
        DownloadFailureKind::Unrequested,
    ] {
        assert!(kind.preserves_article_retry_budget(), "kind={kind:?}");
    }
    for kind in [
        DownloadFailureKind::ArticleNotFound,
        DownloadFailureKind::ContentOrProtocol,
    ] {
        assert!(!kind.preserves_article_retry_budget(), "kind={kind:?}");
    }
    assert!(
        DownloadFailureKind::EstablishedTransport
            .infrastructure_wait_reason()
            .is_some()
    );
}

#[tokio::test]
async fn generation_wake_requeues_ten_thousand_segments_in_one_batch() {
    const RETRY_COUNT: usize = 10_000;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20020);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Generation Batch", "retry.bin", &[128]),
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    pipeline.pending_retries_by_job.insert(job_id, RETRY_COUNT);
    pipeline.download_wait_by_job.insert(
        job_id,
        DownloadWaitStatus {
            reason: "no eligible NNTP server",
            retry_at_epoch_ms: None,
            pending_count: RETRY_COUNT,
        },
    );

    for segment_number in 0..RETRY_COUNT as u32 {
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };
        pipeline.pending_retries_by_segment.insert(segment_id, 1);
        pipeline.schedule_infrastructure_retry(
            None,
            RetryWork {
                scheduled_pool_generation: pipeline.pool_generation,
                infrastructure_retry: true,
                work: DownloadWork {
                    counters: None,
                    segment_id,
                    message_id: crate::jobs::ids::MessageId::new(&format!(
                        "<batch-{segment_number}>"
                    )),
                    groups: std::sync::Arc::from(Vec::<String>::new()),
                    priority: 0,
                    byte_estimate: 128,
                    retry_count: MAX_SEGMENT_RETRIES,
                    is_recovery: false,
                    completion_critical: false,
                    exclude_servers: vec![0],
                    avoid_server: None,
                },
            },
        );
    }

    pipeline.report_job(job_id);
    assert!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .download_wait_reason
            .is_some()
    );
    let mut job_changes = pipeline.shared_state.subscribe_job_changes();
    let previous_revision = *job_changes.borrow();
    assert_eq!(pipeline.wake_all_infrastructure_retries(), RETRY_COUNT);
    job_changes.changed().await.unwrap();

    assert_eq!(*job_changes.borrow(), previous_revision.wrapping_add(1));
    // The infrastructure wait is gone; what is left is the dispatcher's own
    // gate on the requeued work, and this pipeline has no server.
    assert_eq!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .download_wait_reason
            .as_deref(),
        Some("no_server")
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        RETRY_COUNT
    );
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(pipeline.pending_retries_by_segment.is_empty());
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn stale_generation_transport_failure_is_requeued_without_poisoning_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20015);
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
        segmented_job_spec("Generation Fence", "retry.bin", &[128]),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    pipeline.pool_generation = 2;
    hold_on_test_lane(&mut pipeline, &[segment_id]);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 1,
            segment_id,
            data: Err(DownloadError::fetch(
                DownloadFailureKind::EstablishedTransport,
                "old generation connection reset",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: MAX_SEGMENT_RETRIES + 10,
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    assert_eq!(state.failed_bytes, 0);
    let queued = state.download_queue.drain_all();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].segment_id, segment_id);
    assert_eq!(queued[0].retry_count, MAX_SEGMENT_RETRIES + 10);
    assert!(queued[0].exclude_servers.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .nntp_generation_recovery_requeues
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn stale_generation_success_does_not_update_new_lane_health() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.pool_generation = 2;
    hold_on_test_lane(&mut pipeline, &[held_segment(JobId(20017), 0, 0)]);
    pipeline.active_download_connections = 1;

    pipeline.release_download_result(&DownloadResult {
        lane_id: 0,
        job_id: JobId(20017),
        runtime_generation: 1,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(20017),
                file_index: 0,
            },
            segment_number: 0,
        },
        data: Ok(DownloadPayload::Raw(Bytes::from_static(b"article"))),
        attempts: Vec::new(),
        lane_observation: Some(DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 2 },
            supports_pipelining: true,
            latency_sample: Some(Duration::from_millis(5)),
            cold: false,
            transfer: Some(Duration::from_millis(20)),
            payload_bytes: 7,
            policy_elapsed: Duration::from_millis(25),
            pressure_clear: true,
            batch_complete: true,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
        }),
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: true,
    });

    assert_eq!(
        pipeline
            .metrics
            .download_pipeline_trial_success_total
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn terminal_accounting_is_idempotent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
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
        segmented_job_spec("Terminal Accounting", "retry.bin", &[128, 10_000]),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.book_failed_segment(segment_id);
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 128);
    assert_eq!(pipeline.segment_terminal_states.len(), 1);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 128);

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Failed {
            error: "retry requested".to_string(),
        };
    }
    pipeline.reprocess_job(job_id).await.unwrap();
    assert!(!pipeline.segment_terminal_states.contains_key(&segment_id));
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 0);

    // A reprocess is a new admission: the graph fetches every article again,
    // so the article is pending and its loss books its declared bytes afresh.
    // Health policy is not what is under test: a recovery set large enough to
    // make the critical threshold zero keeps the loss in the deferral arm
    // instead of aborting the job out from under the ledger.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.par2_bytes = state.spec.total_bytes;
    }
    let booked = u64::from(pipeline.jobs[&job_id].spec.files[0].segments[0].bytes);
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, booked);
    assert!(pipeline.segment_terminal_states.contains_key(&segment_id));
    pipeline.purge_terminal_job_runtime(job_id);
    assert!(!pipeline.segment_terminal_states.contains_key(&segment_id));
}

// Every segment of a file dies, some of them only after a retry that also
// died. The ledger is the sum of the declared segment sizes, counted once
// each — not once per attempt.
#[tokio::test]
async fn a_file_whose_every_segment_dies_books_each_declared_size_exactly_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    {
        // Health policy is not what is under test. A recovery set large enough
        // to make the critical threshold zero keeps a wholly dead file in the
        // deferral arm instead of aborting the job out from under the ledger.
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.par2_bytes = state.spec.total_bytes;
    }

    let segment = |segment_number| SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number,
    };

    // Each ordinal is retired twice over, through both wire-outcome paths, in
    // the order a real pass produces: a first attempt gives up, the retry that
    // followed it gives up as well.
    for segment_number in 0..segment_sizes.len() as u32 {
        pipeline.book_terminal_segment(segment(segment_number), SegmentTerminalState::Missing);
        pipeline.book_terminal_segment(
            segment(segment_number),
            SegmentTerminalState::RetriesExhausted,
        );
    }

    let expected: u64 = segment_sizes.iter().map(|bytes| *bytes as u64).sum();
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, expected);
    assert_eq!(pipeline.derived_failed_bytes(job_id), expected);
    assert_eq!(
        pipeline.segment_terminal_states.len(),
        segment_sizes.len(),
        "a segment holds one terminal state, and the first one it reached"
    );
    assert!(
        pipeline
            .segment_terminal_states
            .values()
            .all(|state| *state == SegmentTerminalState::Missing)
    );
}

// A mixed pass: some segments fail, retry, and fail again; others fail once
// and then arrive. Only the terminal failures are booked, and the segments
// that landed contribute nothing however many times they failed first.
#[tokio::test]
async fn segments_that_arrive_after_a_failure_leave_the_ledger_untouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20019);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.par2_bytes = state.spec.total_bytes;
    }

    let segment = |segment_number| SegmentId {
        file_id,
        segment_number,
    };

    // Ordinals 1 and 3 fail once, then arrive on the retry.
    pipeline.book_terminal_segment(segment(0), SegmentTerminalState::Missing);
    pipeline.book_terminal_segment(segment(2), SegmentTerminalState::DecodeExhausted);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(file_id).unwrap();
        file.commit_segment(1, 2_000).unwrap();
        file.commit_segment(3, 4_000).unwrap();
    }
    // A losing racer for an ordinal that already landed reports afterwards.
    pipeline.book_terminal_segment(segment(1), SegmentTerminalState::RetriesExhausted);
    pipeline.book_terminal_segment(segment(3), SegmentTerminalState::Missing);
    // And the two dead ordinals exhaust their own retries.
    pipeline.book_terminal_segment(segment(0), SegmentTerminalState::RetriesExhausted);
    pipeline.book_terminal_segment(segment(2), SegmentTerminalState::RetriesExhausted);

    assert_eq!(pipeline.jobs.get(&job_id).unwrap().failed_bytes, 4_000);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 4_000);
    assert_eq!(pipeline.segment_terminal_states.len(), 2);
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
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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
    let cancelled = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[large_segment, tail_segment]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: large_segment.file_id.job_id,
            runtime_generation: 0,
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
            lane_id: 0,
            job_id: tail_segment.file_id.job_id,
            runtime_generation: 0,
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
    let cancelled = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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

    let retry = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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

    let retry = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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
    let cancelled = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

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
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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

    let retry = pipeline
        .retry_rx
        .recv()
        .await
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
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    // A miss on the deep server (index 1) plus the retention-excluded
    // short server (index 0) exhausts both servers: the article books
    // failed bytes instead of retrying forever.
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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
async fn enqueued_work_no_server_may_fetch_is_booked_missing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let job_id = JobId(40153);
    let spec = segmented_job_spec("Unservable Requeue", "solo.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.enqueue_download_work(DownloadWork {
        counters: None,
        segment_id,
        message_id: crate::jobs::ids::MessageId::new("unservable@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 100,
        byte_estimate: 128,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: vec![0],
        avoid_server: None,
    });

    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.download_queue.is_empty() && state.failed_bytes == 128),
        "the article must be booked missing, not queued (job may already be archived by health)"
    );
}

#[tokio::test]
async fn fully_retention_excluded_queue_is_retired_when_the_scheduler_is_asked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40154);
    let spec = segmented_job_spec(
        "Beyond All Retention Queue",
        "ancient.bin",
        &[128, 128, 128, 128],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[5, 5]));
    age_job_days(&mut pipeline, job_id, 10);
    assert_eq!(pipeline.jobs[&job_id].download_queue.len(), 4);

    let pressure = pipeline.refresh_download_pressure();
    let handout = pipeline.next_works(0, 4, None, pressure);
    assert!(matches!(
        handout,
        crate::pipeline::download::scheduler::Handout::Idle
    ));
    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        4
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_none_or(|state| state.download_queue.is_empty() && state.failed_bytes == 512),
        "every article must be booked missing (job may already be archived by health)"
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
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    // Every server is retention-excluded, so the lane path never fetches —
    // the failure surfaces as LaneUnavailable. It must be booked as a
    // missing article, not requeued without budget forever.
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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
                route_feedback: None,
                connection_health: None,
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

    let work = pipeline
        .retry_rx
        .recv()
        .await
        .expect("retry channel must stay open")
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

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 2, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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

    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 1)]);
    pipeline.active_download_passes.insert(job_id);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            data: Err(DownloadError::fetch(
                DownloadFailureKind::ContentOrProtocol,
                "malformed BODY response",
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
            counters: None,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("recovery-0@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
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

// The job an article belongs to rides on the result itself, so per-job
// accounting never has to ask which job holds the lane the article arrived
// on. This result closes its lane — it carries the lane's last outstanding
// work and its connection slot — so the owner entry is struck from the map
// before any of that accounting runs, and the job is still booked correctly.
#[tokio::test]
async fn released_result_books_its_own_job_after_its_lane_owner_is_gone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40711);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical: false,
            server_idx: Some(0),
            connection: true,
            outstanding: HashMap::from([(
                segment_id,
                DownloadWork {
                    counters: None,
                    segment_id,
                    message_id: MessageId::new("carried-article@example.com"),
                    groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
                    priority: 0,
                    byte_estimate: 128,
                    retry_count: 0,
                    is_recovery: false,
                    completion_critical: false,
                    exclude_servers: Vec::new(),
                    avoid_server: None,
                },
            )]),
        },
    );
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_connections = 1;
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);

    assert!(pipeline.release_download_result(&DownloadResult {
        lane_id,
        job_id,
        runtime_generation: pipeline.pool_generation,
        segment_id,
        data: Ok(DownloadPayload::Raw(Bytes::from_static(b"carried-article"))),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: true,
    }));

    assert!(
        !pipeline.download_lane_owners.contains_key(&lane_id),
        "the result that closes a lane retires its owner before the job is booked"
    );
    assert!(
        !pipeline.active_downloads_by_job.contains_key(&job_id),
        "the result's own job is what the in-flight count is charged back to"
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&job_id),
        "the result's own job is what the connection slot is returned to"
    );
    assert!(
        pipeline.job_last_download_activity.contains_key(&job_id),
        "download activity is noted against the result's own job"
    );
}

// An article that already failed on one server and 430s on the other is
// missing everywhere. The completion path can only see that when the result
// carries the article's own exclusions: a lane's batch is cut for one job,
// not for one exclusion set, so the lease's set says nothing about which
// servers this article has already been refused by. Reporting the lease's
// set instead leaves the exclusion set one server wide forever, and because
// a sourced 430 spends no retry budget and waits no delay, the article
// ping-pongs between the two servers for as long as the job lives.
#[tokio::test]
async fn article_not_found_exhausts_when_the_work_already_failed_elsewhere() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40160);
    let spec = segmented_job_spec("Missing Everywhere", "gone.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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
            // What the article carries after its first 430 on server 0.
            exclude_servers: vec![0],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .metrics
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "both servers have now refused the article, so it is missing"
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

// A 430 whose server is already in the article's exclusion set taught the
// retry nothing: the next attempt asks the same servers, gets the same
// answer, spends no retry budget and waits no delay. Book it rather than
// spin, even with servers left over in the pool.
#[tokio::test]
async fn article_not_found_that_learns_no_new_server_books_instead_of_retrying() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40161);
    let spec = segmented_job_spec("Nothing Learned", "loop.bin", &[128, 128, 128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0, 0]));

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    hold_on_test_lane(&mut pipeline, &[held_segment(job_id, 0, 0)]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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
            exclude_servers: vec![1],
            release_connection_slot: true,
        })
        .await;

    assert_eq!(
        pipeline
            .metrics
            .articles_not_found_without_new_server
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "a 430 that adds no server to the exclusion set must be counted"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
}

// A server reachable only through an egress whose quota is spent cannot dial,
// so the work parks on that egress and the block names it, instead of being
// retried at once onto a route that has no leg left to carry it.
#[tokio::test]
async fn egress_quota_refusal_parks_when_no_server_has_a_leg_in_quota() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(21002);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let spec = segmented_job_spec("Egress Quota Park", "egress-park.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

    let db = Database::open_in_memory().unwrap();
    let mut system = db
        .list_egress_interfaces()
        .unwrap()
        .into_iter()
        .find(|egress| egress.id == crate::proxies::SYSTEM_EGRESS_ID)
        .expect("the system egress is seeded");
    system.download_quota = crate::servers::ServerDownloadQuotaConfig {
        enabled: true,
        limit_bytes: 128,
        period: crate::servers::ServerDownloadQuotaPeriod::OneTime,
        ..Default::default()
    };
    db.update_egress_interface(&system).unwrap();
    let policy = std::sync::Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(db.clone(), &[])
            .unwrap(),
    );
    let runtime = crate::proxies::ProxyRuntime::with_quota_policy(
        db,
        tokio::runtime::Handle::current(),
        Some(policy.clone()),
    )
    .unwrap();
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = crate::SchedulerHandle::new(events, pipeline.shared_state.clone());
    handle.set_proxy_runtime(runtime.clone());
    pipeline.shared_state.set_server_transfer_policy(policy);

    // The remaining allowance cannot fit the next body, even though the
    // byte counter has not reached the quota limit.
    let egress = runtime
        .network
        .egress_controls
        .control(weaver_nntp::transfer::StableServerId(
            crate::proxies::SYSTEM_EGRESS_ID,
        ));
    let mut permit = egress.try_reserve(96).unwrap();
    permit.record_blocking(96);
    permit.finish();
    let rejection = egress
        .try_reserve(128)
        .err()
        .expect("the spent egress must refuse the next body");
    assert_eq!(
        rejection.scope,
        weaver_nntp::transfer::TransferScope::Egress
    );

    // The only server takes the default route, whose one leg is that egress.
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "egress-park.example.com".to_string(),
                ..Default::default()
            },
            stable_id: weaver_nntp::transfer::StableServerId(17),
            transfer_control: None,
            max_connections: 1,
            group: 0,
            backfill: false,
            retention_days: 0,
        }],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 0,
        soft_timeout: Duration::from_secs(1),
    }));
    let failure = DownloadFailure::from_lane_acquire_failure(Some(
        &weaver_nntp::NntpError::quota_blocked(rejection),
    ));

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
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

    assert_eq!(
        pipeline.egress_quota_parked.get(&segment_id),
        Some(&crate::proxies::SYSTEM_EGRESS_ID)
    );
    let block = pipeline.shared_state.download_block();
    assert_eq!(
        block.kind,
        crate::jobs::handle::DownloadBlockKind::EgressQuota
    );
    assert_eq!(block.egress_id, Some(crate::proxies::SYSTEM_EGRESS_ID));
    assert_eq!(block.used_bytes, 96);

    // A capacity notification is a prompt to reconsider, not proof that the
    // exhausted route can carry the article. Reapplying the same policy
    // wakes its waiter without giving the egress any additional allowance.
    let mut capacity_wait = Box::pin(Pipeline::wait_for_egress_quota_capacity(
        egress.subscribe_capacity_changes(),
        runtime.clone(),
        pipeline.nntp.clone(),
        egress.quota_rejection_for_dispatch(128).unwrap(),
        Vec::new(),
    ));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(capacity_wait.as_mut(), &mut context).is_pending());
    let snapshot = egress.snapshot();
    egress.update_config(weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes: 128,
            generation: snapshot.quota_generation,
            retry_at: None,
        }),
    });
    assert!(std::future::Future::poll(capacity_wait.as_mut(), &mut context).is_pending());
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::EgressQuota
    );
    egress.update_config(weaver_nntp::transfer::ServerTransferConfig {
        rate_bytes_per_sec: 0,
        quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
            limit_bytes: 256,
            generation: snapshot.quota_generation,
            retry_at: None,
        }),
    });
    assert!(std::future::Future::poll(capacity_wait.as_mut(), &mut context).is_ready());
    let retry = pipeline.retry_rx.recv().await.unwrap();
    assert_eq!(retry.work.segment_id, segment_id);
    pipeline.receive_retry_work(retry);
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::None
    );

    pipeline.jobs.remove(&job_id);
    pipeline.shared_state.publish_jobs(Vec::new());
    runtime.network.shutdown().await;
}

#[tokio::test]
async fn recovery_metadata_bootstrap_holds_graph_payload_until_indexes_settle() {
    for par3 in [false, true] {
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
        let job_id = JobId(40146);
        let payload = (0..(7 * 128))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let index = build_test_par2_index("payload.mkv", &payload, 64);
        let payload_segments = (0..7)
            .map(|number| {
                segment_spec! {
                    number: number,
                    bytes: 128,
                    message_id: format!("graph-payload-{number}@example.com"),
                }
            })
            .collect::<Vec<_>>();
        let spec = JobSpec {
            name: "Graph PAR2 Bootstrap".to_string(),
            password: None,
            total_bytes: (index.len() + payload.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: if par3 {
                        "00-index.par3"
                    } else {
                        "00-index.par2"
                    }
                    .to_string(),
                    role: if par3 {
                        FileRole::Par3 { is_index: true }
                    } else {
                        FileRole::Par2 {
                            is_index: true,
                            recovery_block_count: 0,
                        }
                    },
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: index.len() as u32,
                        message_id: "graph-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: "payload.mkv".to_string(),
                    role: FileRole::Standalone,
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: payload_segments,
                },
            ],
        };
        insert_active_job(&mut pipeline, job_id, spec).await;
        let queued = pipeline.jobs.get(&job_id).unwrap().download_queue.len();
        assert_eq!(queued, 8);

        // The namespace holds the payload an index to come will place: only
        // the index leases until it has been read, and nothing is refused.
        let mut leased = Vec::new();
        while let Some(lease) = pipeline.lease_for_server_for_test(0) {
            leased.extend(lease.works.into_iter().map(|work| work.segment_id));
        }
        assert_eq!(
            leased
                .iter()
                .map(|id| id.file_id.file_index)
                .collect::<Vec<_>>(),
            vec![0],
            "{leased:?}"
        );
        assert_eq!(
            pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
            queued - 1
        );
        let graph = pipeline.graph_jobs.get_mut(&job_id).unwrap();
        graph
            .lost(
                weaver_graph::VolumeIndex(0),
                0,
                &mut crate::pipeline::graph::status::RepairSlots::default(),
                0,
            )
            .unwrap();
        let declared = [(
            weaver_graph::MemberId(0),
            if par3 {
                crate::pipeline::graph::metadata::RecoveryFormat::Par3
            } else {
                crate::pipeline::graph::metadata::RecoveryFormat::Par2
            },
        )];
        assert_eq!(
            graph.indexes_to_come(declared),
            None,
            "a missing index must release payload"
        );
    }
}

// A pipelined BODY batch that ended with its connection torn down.
fn unclean_batch_result(job_id: JobId, segment_number: u32) -> DownloadResult {
    DownloadResult {
        lane_id: 0,
        job_id,
        runtime_generation: 0,
        segment_id: held_segment(job_id, 0, segment_number),
        data: Err(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::EstablishedTransport,
            "connection closed",
        ))),
        attempts: Vec::new(),
        lane_observation: Some(DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 2 },
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: Duration::ZERO,
            pressure_clear: true,
            batch_complete: true,
            batch_clean: false,
            unresolved_count: 1,
            connection_discarded: true,
        }),
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: false,
    }
}

#[tokio::test]
async fn a_batch_cut_short_by_cancellation_is_not_an_unclean_batch() {
    // Shutdown, a job cancel and a pause all tear down lanes mid-batch. None
    // of that says anything about the server, so none of it may lower the
    // depth or pin the server; a genuine transport fault still does.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Cut Short", "payload.bin", &[64, 64, 64, 64, 64, 64, 64]),
    )
    .await;
    pipeline.download_lane_runtime.servers.insert(
        0,
        crate::pipeline::download::transport::ServerPipelineExplorer::seeded(Some(2), None, None),
    );
    let depth = |pipeline: &Pipeline| {
        let explorer = pipeline.download_lane_runtime.servers.get(&0).unwrap();
        (explorer.current_depth(), explorer.pinned_sequential())
    };
    assert_eq!(depth(&pipeline), (2, false));
    let queue = pipeline.shared_state.queue.clone();
    let set_row = |paused: Option<bool>, cancelled: bool| {
        queue.write(|inner| {
            let row = inner.jobs.iter_mut().find(|row| row.id == job_id).unwrap();
            row.intents.paused = paused;
            row.intents.cancelled =
                cancelled.then_some(crate::jobs::handle::CancellationOrigin::User);
            crate::pipeline::queue::Change::changed(())
        })
    };

    let mut segment = 0;
    let mut release_unclean = |pipeline: &mut Pipeline| {
        hold_on_test_lane(pipeline, &[held_segment(job_id, 0, segment)]);
        pipeline.release_download_result(&unclean_batch_result(job_id, segment));
        segment += 1;
    };

    // Shutdown.
    queue.write(|inner| {
        inner.globals.shutting_down = true;
        crate::pipeline::queue::Change::changed(())
    });
    release_unclean(&mut pipeline);
    release_unclean(&mut pipeline);
    queue.write(|inner| {
        inner.globals.shutting_down = false;
        crate::pipeline::queue::Change::changed(())
    });
    // The global pause.
    queue.write(|inner| {
        inner.globals.paused = true;
        crate::pipeline::queue::Change::changed(())
    });
    release_unclean(&mut pipeline);
    queue.write(|inner| {
        inner.globals.paused = false;
        crate::pipeline::queue::Change::changed(())
    });
    // The job's own pause, then its cancel.
    set_row(Some(true), false);
    release_unclean(&mut pipeline);
    set_row(None, true);
    assert!(!pipeline.unclean_batch_is_server_fault(job_id));
    set_row(None, false);

    assert_eq!(
        depth(&pipeline),
        (2, false),
        "a batch cut short by cancellation must neither lower depth nor pin the server"
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pipeline_trial_failure_total
            .load(Ordering::Relaxed),
        0
    );

    // The same batch while the job is meant to be downloading is a server
    // fault, and the explorer acts on it.
    assert!(pipeline.unclean_batch_is_server_fault(job_id));
    release_unclean(&mut pipeline);
    assert_eq!(
        depth(&pipeline),
        (1, false),
        "a genuine BODY failure still lowers depth"
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pipeline_trial_failure_total
            .load(Ordering::Relaxed),
        1
    );
}

// The dispatcher's own gate is the item's wait reason, and a job it holds
// off every lane reads as queued rather than downloading.
#[tokio::test]
async fn a_job_the_dispatcher_holds_off_every_lane_reports_why_on_the_queue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20032);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Pressure Hold", "pressure-hold.bin", &[1024]),
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Downloading;
    pipeline.report_job(job_id);
    // This pipeline has no server to dispatch to.
    assert_eq!(pipeline.job_dispatch_hold_reason(job_id), Some("no_server"));

    pipeline.metrics.download_pressure_state.store(
        crate::DownloadPressureState::Hard.as_code(),
        Ordering::Relaxed,
    );
    assert_eq!(
        pipeline
            .job_report(job_id)
            .unwrap()
            .download_wait_reason
            .as_deref(),
        Some("byte_pressure")
    );
    pipeline.report_changed_dispatch_holds();
    let view = |pipeline: &Pipeline| {
        pipeline.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .find(|row| row.id == job_id)
                .map(|row| row.info(&inner.globals))
                .unwrap()
        })
    };
    let held = view(&pipeline);
    assert_eq!(held.download_wait_reason.as_deref(), Some("byte_pressure"));
    assert_eq!(held.download_state, crate::DownloadState::Queued);

    // A lane holding the job is not a hold.
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    assert_eq!(pipeline.job_dispatch_hold_reason(job_id), None);
    pipeline.report_changed_dispatch_holds();
    let held_lane = view(&pipeline);
    assert_eq!(held_lane.download_wait_reason, None);
    assert_eq!(held_lane.download_state, crate::DownloadState::Downloading);
    pipeline.active_download_connections_by_job.remove(&job_id);

    // The pressure lifts: the next pass reports the gate still standing.
    pipeline.metrics.download_pressure_state.store(
        crate::DownloadPressureState::Clear.as_code(),
        Ordering::Relaxed,
    );
    pipeline.report_changed_dispatch_holds();
    assert_eq!(
        view(&pipeline).download_wait_reason.as_deref(),
        Some("no_server")
    );
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

    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );

    assert_eq!(
        pipeline
            .download_wait_by_job
            .get(&job_id)
            .map(|wait| wait.reason),
        Some("provider connection capacity unavailable")
    );
    tokio::task::yield_now().await;
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("generation wake should requeue parked infrastructure work");
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES);
    assert_eq!(retry.exclude_servers, vec![0]);
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
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

    hold_on_test_lane(&mut pipeline, &[segment_id]);
    pipeline.active_download_passes.insert(job_id);

    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
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
    assert_eq!(
        pipeline
            .metrics
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    tokio::task::yield_now().await;
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline
            .download_wait_by_job
            .get(&job_id)
            .map(|wait| wait.reason),
        Some("no eligible NNTP server")
    );

    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    let retry = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("generation wake should requeue parked pre-BODY work without retry charge");
    assert_eq!(retry.segment_id, segment_id);
    assert_eq!(retry.retry_count, MAX_SEGMENT_RETRIES + 1);
    assert_eq!(retry.exclude_servers, vec![0, 1]);
    assert_eq!(
        pipeline
            .metrics
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        0
    );
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
}
