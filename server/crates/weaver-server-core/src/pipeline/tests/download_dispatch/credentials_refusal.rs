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
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
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
    // An auth refusal is an infrastructure retry: it parks for its delay.
    // The refusing server leaves the article's candidates through the
    // pool's refusal, not through the article's exclusions, which record
    // only servers that lack the article.
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    let due = pipeline.infrastructure_retries.next_deadline().unwrap();
    let ready = pipeline.infrastructure_retries.take_due(due);
    assert_eq!(ready.len(), 1);
    assert!(ready[0].work.exclude_servers.is_empty());
    let retention = pipeline.job_retention_excludes(job_id);
    assert!(!Pipeline::server_may_serve(
        pipeline.nntp.pool(),
        0,
        &ready[0].work.exclude_servers,
        &retention,
    ));
    assert!(Pipeline::server_may_serve(
        pipeline.nntp.pool(),
        1,
        &ready[0].work.exclude_servers,
        &retention,
    ));
    assert!(!pipeline.no_server_can_serve(job_id, &ready[0].work.exclude_servers));
}

// A fill server refusing the credentials does not lack the article, so it
// never opens the backfill tier: with a backfill server the only other
// server, the article is booked at once and the backfill server is never
// asked for it.
#[tokio::test]
async fn a_fill_server_refusing_credentials_books_the_article_and_leaves_backfill_untouched() {
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
    let job_id = JobId(41962);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Refused Fill", "payload.bin", &[128]),
    )
    .await;
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(0, true);
    assert!(pipeline.nntp.pool().refuses_credentials(0));
    // Before any refusal reaches the article, nothing may serve it: the
    // fill server refuses and the backfill tier stays closed.
    assert!(pipeline.no_server_can_serve(job_id, &[]));
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
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

    // The article is booked, not parked for a retry the backfill server
    // would take.
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert_eq!(pipeline.pending_retries_by_job.get(&job_id).copied(), None);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        1
    );
    // The backfill server stays closed to the article.
    let retention = pipeline.job_retention_excludes(job_id);
    assert!(!Pipeline::server_may_serve(
        pipeline.nntp.pool(),
        1,
        &[],
        &retention,
    ));
}
