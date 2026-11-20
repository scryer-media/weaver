// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;
use tokio::time::Instant;
use weaver_server_composition::{JobId, NzbFileId, SegmentId};
use weaver_server_pipeline::DownloadQueue;
use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::{DispatchFixture, owned_failures::FailedFetch},
    segmented_job_spec,
};

#[tokio::test]
async fn unavailable_provider_recovery_does_not_leave_602_retries_parked_forever() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp).await;
    let registry = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        weaver_nntp::transfer::StableServerId(0),
        weaver_nntp::transfer::ServerTransferConfig {
            closed: false,
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
    pipeline.set_client(weaver_nntp::NntpClient::new(config));
    let job_id = JobId(30804);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Provider recovery", "payload.bin", &[128; 602]),
        )
        .await;
    pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    assert!(matches!(
        pipeline.body_availability().await,
        weaver_nntp::pool::BodyServerAvailability::Blocked
    ));
    for segment_number in 0..602 {
        let segment = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };
        pipeline
            .failed_fetch(
                FailedFetch::unavailable(segment, "failed to acquire BODY lane")
                    .without_lane_release(),
            )
            .await;
    }
    assert_eq!(pipeline.pending_retries(job_id), Some(602));
    assert!(
        pipeline
            .take_due_retries(Instant::now() + Duration::from_secs(1))
            .is_empty(),
        "blocked providers must not cause a busy retry loop"
    );
    drop(reservation);
    assert!(matches!(
        pipeline.body_availability().await,
        weaver_nntp::pool::BodyServerAvailability::Eligible
    ));
    // No rebuild occurs when transient quota reservations are refunded. A
    // retry parked without either a wake or a deadline never gets selected.
    assert!(
        pipeline.retry_deadline().is_some(),
        "recoverable provider unavailability must retain a liveness deadline"
    );
    let ready = pipeline.take_due_retries(Instant::now() + Duration::from_secs(60));
    assert_eq!(ready.len(), 602);
    for retry in ready {
        assert_eq!(retry.work().retry_count, 0);
        pipeline.requeue_retry(retry);
    }
    assert_eq!(
        pipeline
            .job(job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        602
    );
    assert!(pipeline.pending_retries(job_id).is_none());
    assert!(!pipeline.has_download_wait(job_id));
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
}
