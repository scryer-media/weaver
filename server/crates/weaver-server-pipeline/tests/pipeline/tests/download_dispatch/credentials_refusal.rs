// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server that has refused the credentials for its whole give-up time has
// answered for the articles it was asked for; a server quarantined for
// transport or capacity has not, and neither has one that began refusing
// the login within that time. The first is a verdict, booked; the others
// are a wait.

use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::{DispatchFixture, owned_failures::FailedFetch},
    segmented_job_spec,
};
use weaver_server_pipeline::{DownloadQueue, JobId, NzbFileId, SegmentId};

fn held_segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

#[tokio::test]
async fn a_credentials_refusal_retries_on_a_server_that_may_still_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention_client(&[0, 0]);
    let job_id = JobId(41961);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Refused Once", "payload.bin", &[128]),
        )
        .await;
    pipeline.refuse_login(0).await;
    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::auth(held_segment(job_id, 0, 0), "server 0 refuses credentials")
                .on_server(0),
        )
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(pipeline.pending_retries(job_id), Some(1));
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.failed_bytes),
        Some(0)
    );
    // An auth refusal is an infrastructure retry: it parks for its delay.
    // The refusing server leaves the article's candidates through the
    // pool's refusal, not through the article's exclusions, which record
    // only servers that lack the article.
    assert_eq!(pipeline.parked_retry_count(), 1);
    let due = pipeline.retry_deadline().unwrap();
    let ready = pipeline.take_due_retry_works(due);
    assert_eq!(ready.len(), 1);
    assert!(ready[0].exclude_servers.is_empty());
    let retention = pipeline.retention_excludes(job_id);
    assert!(!pipeline.server_may_serve(0, ready[0].exclude_servers, retention,));
    assert!(pipeline.server_may_serve(1, ready[0].exclude_servers, retention,));
    assert!(!pipeline.no_server_can_serve(job_id, ready[0].exclude_servers));
}

// Every article of a file refused by one server waits to retry on the other,
// and every one of those retries holds the file's one group list.
#[tokio::test]
async fn refused_articles_of_a_file_retry_holding_its_one_group_list() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention_client(&[0, 0]);
    let job_id = JobId(41962);
    let segments = 64u32;
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Refused File", "payload.bin", &vec![128; segments as usize]),
        )
        .await;
    pipeline.record_server_failure(0, true).await;
    let file_id = held_segment(job_id, 0, 0).file_id;
    let shared = pipeline.drain_download_queue_and_get_groups(file_id);
    let held: Vec<_> = (0..segments)
        .map(|number| held_segment(job_id, 0, number))
        .collect();
    pipeline.hold_segments(&held);
    pipeline.open_pass(job_id);

    for segment_id in held {
        pipeline
            .failed_fetch(
                FailedFetch::auth(segment_id, "server 0 refuses credentials").on_server(0),
            )
            .await;
    }

    let ready = pipeline.take_parked_retry_works();
    assert_eq!(ready.len(), segments as usize);
    assert!(
        ready
            .iter()
            .all(|retry| Arc::ptr_eq(&retry.groups, &shared))
    );
}

// A fill server refusing the credentials does not lack the article, so it
// never opens the backfill tier: with a backfill server the only other
// server, the article is booked at once and the backfill server is never
// asked for it.
#[tokio::test]
async fn a_fill_server_refusing_credentials_books_the_article_and_leaves_backfill_untouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
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
    pipeline.set_client(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers,
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    let job_id = JobId(41962);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Refused Fill", "payload.bin", &[128]),
        )
        .await;
    pipeline.refuse_login(0).await;
    // Before any refusal reaches the article, nothing may serve it: the
    // fill server refuses and the backfill tier stays closed.
    assert!(pipeline.no_server_can_serve(job_id, weaver_nntp::ServerSet::EMPTY));
    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::auth(held_segment(job_id, 0, 0), "server 0 refuses credentials")
                .on_server(0),
        )
        .await;

    // The article is booked, not parked for a retry the backfill server
    // would take.
    assert_eq!(pipeline.parked_retry_count(), 0);
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(Ordering::Relaxed),
        1
    );
    // The backfill server stays closed to the article.
    let retention = pipeline.retention_excludes(job_id);
    assert!(!pipeline.server_may_serve(1, weaver_nntp::ServerSet::EMPTY, retention,));
}

// A backfill server refusing the login holds nothing: its refusal is the
// answer at once, with no give-up time. Once the fill server has said "not
// found", the article it alone could still bring is booked, not held for
// it.
#[tokio::test]
async fn a_backfill_server_refusing_the_login_holds_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let server = |host: &str, backfill| weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: host.into(),
            ..Default::default()
        },
        max_connections: 2,
        backfill,
        ..weaver_nntp::pool::ServerPoolConfig::default()
    };
    pipeline.set_client(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![
                server("fill.example.com", false),
                server("backfill.example.com", true),
            ],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    let job_id = JobId(41963);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Refused Backfill", "payload.bin", &[128]),
        )
        .await;
    pipeline.refuse_login_fresh(1).await;
    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::missing(held_segment(job_id, 0, 0), "430 no such article").on_server(0),
        )
        .await;

    assert_eq!(pipeline.parked_retry_count(), 0);
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline.login_refusal_words(job_id).as_deref(),
        Some(
            "server backfill.example.com:563 refuses the login and was never asked for the \
             articles no other server had"
        )
    );
}
