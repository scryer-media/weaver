use super::*;

#[tokio::test]
async fn unavailable_provider_recovery_does_not_leave_602_retries_parked_forever() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let registry = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        weaver_nntp::transfer::StableServerId(0),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 1,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let reservation = control.try_reserve(1).unwrap();
    let mut config = weaver_nntp::client::NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port: 1,
            tls: false,
            ..Default::default()
        },
        100,
    );
    config.servers[0].transfer_control = Some(control.clone());
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(config));
    let job_id = JobId(30804);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Provider recovery", "payload.bin", &[128; 602]),
    )
    .await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    assert!(matches!(
        pipeline.nntp.body_server_availability(&[], &[], 0).await,
        weaver_nntp::pool::BodyServerAvailability::Blocked
    ));
    for segment_number in 0..602 {
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                runtime_generation: 0,
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                    segment_number,
                },
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::LaneUnavailable,
                    "failed to acquire BODY lane",
                )),
                attempts: vec![],
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: vec![],
                release_connection_slot: false,
            })
            .await;
    }
    assert_eq!(pipeline.pending_retries_by_job[&job_id], 602);
    assert!(
        pipeline
            .infrastructure_retries
            .take_due(tokio::time::Instant::now() + Duration::from_secs(1))
            .is_empty(),
        "blocked providers must not cause a busy retry loop"
    );
    drop(reservation);
    assert!(matches!(
        pipeline.nntp.body_server_availability(&[], &[], 0).await,
        weaver_nntp::pool::BodyServerAvailability::Eligible
    ));
    // No rebuild occurs when transient quota reservations are refunded. A
    // retry parked without either a wake or a deadline never gets selected.
    assert!(
        pipeline.infrastructure_retries.next_deadline().is_some(),
        "recoverable provider unavailability must retain a liveness deadline"
    );
    let ready = pipeline
        .infrastructure_retries
        .take_due(tokio::time::Instant::now() + Duration::from_secs(60));
    assert_eq!(ready.len(), 602);
    for retry in ready {
        assert_eq!(retry.work.retry_count, 0);
        pipeline.receive_retry_work(retry);
    }
    assert_eq!(pipeline.jobs[&job_id].download_queue.len(), 602);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(!pipeline.download_wait_by_job.contains_key(&job_id));
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
}
