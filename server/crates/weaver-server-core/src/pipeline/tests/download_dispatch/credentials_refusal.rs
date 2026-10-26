// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server refusing the credentials has answered for the articles it was
// asked for; a server quarantined for transport or capacity has not. The
// first is a verdict, booked; the second is a wait.

use super::*;

#[tokio::test]
async fn a_credentials_refusal_retries_on_a_server_that_may_still_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let job_id = JobId(41961);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Refused Once", "payload.bin", &[128]),
    )
    .await;
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(0, true);
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
                DownloadFailureKind::Auth,
                "server 0 refuses credentials",
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

    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    // An auth refusal is an infrastructure retry: it parks for its delay,
    // and the parked work no longer names the refusing server as a
    // candidate.
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    let due = pipeline.infrastructure_retries.next_deadline().unwrap();
    let ready = pipeline.infrastructure_retries.take_due(due);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].work.exclude_servers, vec![0]);
    assert!(!pipeline.no_server_can_serve(job_id, &ready[0].work.exclude_servers));
}
