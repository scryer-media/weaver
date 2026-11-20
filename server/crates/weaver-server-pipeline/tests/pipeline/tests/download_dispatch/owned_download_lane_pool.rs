// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::Ordering;
use std::time::Duration;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::{
    DispatchFixture,
    ladder_failures::{MAX_SEGMENT_RETRIES, SegmentOutcome},
    owned_failures::FailedFetch,
    scheduler_lanes::Handout,
};
use weaver_server_pipeline::pipeline::tests::{segmented_job_spec, standalone_with_par2_job_spec};
use weaver_server_pipeline::{DownloadQueue, DownloadWork, JobId, JobStatus, NzbFileId, SegmentId};

fn held_segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

#[tokio::test]
async fn transient_retry_backoff_does_not_fail_job_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20008);
    let spec = segmented_job_spec("Retry Backoff Guard", "retry.bin", &[128, 128]);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(FailedFetch::transport(
            held_segment(job_id, 0, 0),
            "connection reset by peer",
        ))
        .await;

    assert_eq!(pipeline.pending_retries(job_id), Some(1));
    assert!(pipeline.completion_checks_empty());
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics()
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );

    assert_eq!(pipeline.wake_all_retries(), 1);
    let retry = pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should remain in the batched infrastructure queue");
    assert_eq!(retry.retry_count, 0);

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);
    pipeline
        .failed_fetch(
            FailedFetch::transport(retry.segment_id, "connection reset by peer")
                .retry_count(MAX_SEGMENT_RETRIES)
                .excluding(retry.exclude_servers),
        )
        .await;

    assert_eq!(pipeline.status(job_id), Some(JobStatus::Downloading));
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);
    assert_eq!(
        pipeline
            .metrics()
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics()
            .parked_infrastructure_work
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(pipeline.wake_all_retries(), 1);
    let retry = pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
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
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention(&[0, 0]);
    let job_id = JobId(20009);
    let spec = segmented_job_spec("Transport Rotation", "rotation.bin", &[128]);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::transport(
                held_segment(job_id, 0, 0),
                "article fetch soft timeout (15s)",
            )
            .on_server(0),
        )
        .await;

    assert_eq!(pipeline.wake_all_retries(), 1);
    let retry = pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
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
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention(&[0]);
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Transport Rotation Solo", "rotation-solo.bin", &[128]);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::transport(
                held_segment(job_id, 0, 0),
                "article fetch soft timeout (15s)",
            )
            .on_server(0),
        )
        .await;

    assert_eq!(pipeline.wake_all_retries(), 1);
    let retry = pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
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
    let job_id = JobId(20011);
    let spec = segmented_job_spec("Transport Rotation Backfill", "rotation-bf.bin", &[128]);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::transport(
                held_segment(job_id, 0, 0),
                "article fetch soft timeout (15s)",
            )
            .on_server(0),
        )
        .await;

    assert_eq!(pipeline.wake_all_retries(), 1);
    let retry = pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .expect("transport retry should requeue");
    assert_eq!(
        retry.avoid_server, None,
        "rotation must not manufacture fill-tier exhaustion"
    );
}

#[tokio::test]
async fn terminal_accounting_is_idempotent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20016);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Terminal Accounting", "retry.bin", &[128, 10_000]),
        )
        .await;
    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.book_failed_segment(segment_id);
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.job(job_id).failed_bytes, 128);
    assert_eq!(pipeline.segment_outcomes().len(), 1);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 128);

    {
        let state = pipeline.job_mut(job_id);
        state.status = JobStatus::Failed {
            error: "retry requested".to_string(),
        };
    }
    pipeline.reprocess_and_activate(job_id).await;
    assert!(pipeline.segment_outcome(segment_id).is_none());
    assert_eq!(pipeline.job(job_id).failed_bytes, 0);

    // A reprocess is a new admission: the graph fetches every article again,
    // so the article is pending and its loss books its declared bytes afresh.
    // Health policy is not what is under test: a recovery set large enough to
    // make the critical threshold zero keeps the loss in the deferral arm
    // instead of aborting the job out from under the ledger.
    {
        let state = pipeline.job_mut(job_id);
        state.par2_bytes = state.spec.total_bytes;
    }
    let booked = u64::from(pipeline.job(job_id).spec.files[0].segments[0].bytes);
    pipeline.book_failed_segment(segment_id);
    assert_eq!(pipeline.job(job_id).failed_bytes, booked);
    assert!(pipeline.segment_outcome(segment_id).is_some());
    pipeline.purge_terminal_runtime(job_id);
    assert!(pipeline.segment_outcome(segment_id).is_none());
}

// Every segment of a file dies, some of them only after a retry that also
// died. The ledger is the sum of the declared segment sizes, counted once
// each — not once per attempt.
#[tokio::test]
async fn a_file_whose_every_segment_dies_books_each_declared_size_exactly_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20018);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    // Health policy is not what is under test. A sibling file ten times the
    // dying one's size keeps the post above its critical line, so the feed
    // never gives it up out from under the ledger.
    let mut spec = segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes);
    let sibling = segmented_job_spec("Silver Horizon", "silver-horizon-extra.mkv", &[10_000; 10]);
    spec.total_bytes += sibling.total_bytes;
    spec.files.extend(sibling.files);
    pipeline.add_job(job_id, spec).await;

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
        pipeline.book_outcome(segment(segment_number), SegmentOutcome::MISSING);
        pipeline.book_outcome(segment(segment_number), SegmentOutcome::RETRIES_EXHAUSTED);
    }

    let expected: u64 = segment_sizes.iter().map(|bytes| *bytes as u64).sum();
    assert_eq!(pipeline.job(job_id).failed_bytes, expected);
    assert_eq!(pipeline.derived_failed_bytes(job_id), expected);
    assert_eq!(
        pipeline.segment_outcomes().len(),
        segment_sizes.len(),
        "a segment holds one terminal state, and the first one it reached"
    );
    assert!(
        pipeline
            .segment_outcomes()
            .all(|state| state == SegmentOutcome::MISSING)
    );
}

// A mixed pass: some segments fail, retry, and fail again; others fail once
// and then arrive. Only the terminal failures are booked, and the segments
// that landed contribute nothing however many times they failed first.
#[tokio::test]
async fn segments_that_arrive_after_a_failure_leave_the_ledger_untouched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20019);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes),
        )
        .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.job_mut(job_id);
        state.par2_bytes = state.spec.total_bytes;
    }

    let segment = |segment_number| SegmentId {
        file_id,
        segment_number,
    };

    // Ordinals 1 and 3 fail once, then arrive on the retry.
    pipeline.book_outcome(segment(0), SegmentOutcome::MISSING);
    pipeline.book_outcome(segment(2), SegmentOutcome::DECODE_EXHAUSTED);
    {
        let state = pipeline.job_mut(job_id);
        let file = state
            .articles
            .active_mut()
            .unwrap()
            .assembly
            .file_mut(file_id)
            .unwrap();
        file.commit_segment(1, 2_000).unwrap();
        file.commit_segment(3, 4_000).unwrap();
    }
    // A losing racer for an ordinal that already landed reports afterwards.
    pipeline.book_outcome(segment(1), SegmentOutcome::RETRIES_EXHAUSTED);
    pipeline.book_outcome(segment(3), SegmentOutcome::MISSING);
    // And the two dead ordinals exhaust their own retries.
    pipeline.book_outcome(segment(0), SegmentOutcome::RETRIES_EXHAUSTED);
    pipeline.book_outcome(segment(2), SegmentOutcome::RETRIES_EXHAUSTED);

    assert_eq!(pipeline.job(job_id).failed_bytes, 4_000);
    assert_eq!(pipeline.derived_failed_bytes(job_id), 4_000);
    assert_eq!(pipeline.segment_outcomes().len(), 2);
}

// What the feed weighs a post on is kept as each article is given up, so
// reading it walks nothing of the post: with the post's article lists
// dropped, as a parked job drops them, the figures read the same, and the
// losses alone are what the reads cost.
#[tokio::test]
async fn the_feed_reads_a_posts_losses_without_walking_its_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20021);
    let segment_sizes = [1_000u32, 2_000, 3_000, 4_000];
    // A sibling ten times the size keeps the post above its critical line.
    let mut spec = segmented_job_spec("Silver Horizon", "silver-horizon.mkv", &segment_sizes);
    let sibling = segmented_job_spec("Silver Horizon", "silver-horizon-extra.mkv", &[10_000; 10]);
    spec.total_bytes += sibling.total_bytes;
    spec.files.extend(sibling.files);
    pipeline.add_job(job_id, spec).await;
    let segment = |segment_number| SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number,
    };
    pipeline.book_outcome(segment(0), SegmentOutcome::MISSING);
    pipeline.book_outcome(segment(2), SegmentOutcome::RETRIES_EXHAUSTED);
    let read = |pipeline: &DispatchFixture| {
        (
            pipeline.missing_bytes(job_id),
            pipeline.derived_failed_bytes(job_id),
            pipeline.graph_article_given_up(segment(2)),
            pipeline.graph_article_given_up(segment(1)),
        )
    };
    assert_eq!(read(&pipeline), (1_000, 4_000, true, false));
    for file in &mut pipeline.job_mut(job_id).spec.files {
        file.segments.clear();
    }
    assert_eq!(read(&pipeline), (1_000, 4_000, true, false));
}

#[tokio::test]
async fn retention_excludes_derive_from_job_age_and_server_windows() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40150);
    let spec = segmented_job_spec("Retention Derivation", "old.bin", &[128]);
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[5, 0]);

    // Unknown post date: never retention-skipped.
    assert!(pipeline.retention_excludes(job_id).is_empty());

    // Older than server 0's five-day window: server 0 is excluded, and the
    // effective set merges with per-article failure excludes without dupes.
    pipeline.age_post(job_id, 10);
    pipeline.clear_retention_excludes(job_id);
    assert_eq!(pipeline.retention_excludes(job_id), [0]);
    assert_eq!(
        pipeline.effective_excludes(job_id, weaver_nntp::ServerSet::from([1])),
        vec![1, 0]
    );
    assert_eq!(
        pipeline.effective_excludes(job_id, weaver_nntp::ServerSet::from([0])),
        vec![0]
    );

    // Younger than every window: nothing excluded.
    pipeline.age_post(job_id, 1);
    pipeline.clear_retention_excludes(job_id);
    assert!(pipeline.retention_excludes(job_id).is_empty());
}

#[tokio::test]
async fn article_not_found_exhaustion_counts_retention_excluded_servers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40151);
    let spec = segmented_job_spec("Retention Exhaustion", "aged.bin", &[128, 128, 128, 128]);
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[5, 0]);
    pipeline.age_post(job_id, 10);

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    // A miss on the deep server (index 1) plus the retention-excluded
    // short server (index 0) exhausts both servers: the article books
    // failed bytes instead of retrying forever.
    pipeline
        .failed_fetch(
            FailedFetch::missing(held_segment(job_id, 0, 0), "article not found").on_server(1),
        )
        .await;

    // Exhaustion (not a retry) is the proof: the non-exhausted path
    // schedules a retry and never bumps articles_not_found. With no PAR2 in
    // the spec the booked failure also fails the job outright.
    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "retention-excluded server must count toward exhaustion"
    );
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert!(
        pipeline
            .job_if_present(job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn enqueued_work_no_server_may_fetch_is_booked_missing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.set_retention(&[0]);
    let job_id = JobId(40153);
    let spec = segmented_job_spec("Unservable Requeue", "solo.bin", &[128]);
    pipeline.add_job(job_id, spec).await;
    pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.enqueue(DownloadWork {
        counters: None,
        segment_id,
        message_id: weaver_server_base::jobs::ids::MessageId::new("unservable@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 100,
        byte_estimate: 128,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: weaver_nntp::ServerSet::from([0]),
        avoid_server: None,
    });

    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert!(
        pipeline.job_if_present(job_id).is_none_or(|state| state
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
            && state.failed_bytes == 128),
        "the article must be booked missing, not queued (job may already be archived by health)"
    );
}

#[tokio::test]
async fn fully_retention_excluded_queue_is_retired_when_the_scheduler_is_asked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40154);
    let spec = segmented_job_spec(
        "Beyond All Retention Queue",
        "ancient.bin",
        &[128, 128, 128, 128],
    );
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[5, 5]);
    pipeline.age_post(job_id, 10);
    assert_eq!(
        pipeline
            .job(job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        4
    );

    let handout = pipeline.ask_handout(0, 4, None);
    assert!(matches!(handout, Handout::Idle));
    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        4
    );
    assert!(
        pipeline.job_if_present(job_id).is_none_or(|state| state
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
            && state.failed_bytes == 512),
        "every article must be booked missing (job may already be archived by health)"
    );
}

#[tokio::test]
async fn fully_retention_excluded_job_books_missing_instead_of_requeueing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40152);
    let spec = segmented_job_spec("Beyond All Retention", "ancient.bin", &[128, 128, 128, 128]);
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[5, 5]);
    pipeline.age_post(job_id, 10);

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    // Every server is retention-excluded, so the lane path never fetches —
    // the failure surfaces as LaneUnavailable. It must be booked as a
    // missing article, not requeued without budget forever.
    pipeline
        .failed_fetch(FailedFetch::unavailable(
            held_segment(job_id, 0, 0),
            "failed to acquire BODY lane",
        ))
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "an unserveable article must be booked missing"
    );
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert!(
        pipeline
            .job_if_present(job_id)
            .is_none_or(|state| state.failed_bytes == 128),
        "failed bytes must be booked (job may already be archived by health)"
    );
}

#[tokio::test]
async fn traced_article_not_found_retries_other_servers_without_retry_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20013);
    let spec = segmented_job_spec("Source Not Found", "retry.bin", &[128]);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::missing(
                held_segment(job_id, 0, 0),
                "article not found on source server",
            )
            .attempts(vec![weaver_nntp::client::FetchAttemptTrace {
                route_feedback: None,
                connection_health: None,
                server_idx: 0,
                remote_ip: None,
                elapsed: Duration::from_millis(5),
                outcome: weaver_nntp::client::FetchAttemptOutcome::NotFound,
                error: Some("article not found".to_string()),
            }]),
        )
        .await;

    // The article is back on the queue at once, for the next server: no
    // retry waits on the channel for a quiet turn.
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert!(pipeline.retry_channel_empty());
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.failed_bytes),
        Some(0)
    );
    assert!(pipeline.completion_checks_empty());

    let state = pipeline.job(job_id);
    let queue = &state.articles.active().unwrap().download_queue;
    assert_eq!(queue.len(), 1);
    let work = queue.peek_first_matching(|_| true).unwrap();
    assert_eq!(work.exclude_servers, &[0]);
    assert_eq!(work.retry_count, 0);
}

#[tokio::test]
async fn recovery_article_not_found_does_not_mark_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(20012);
    let spec = standalone_with_par2_job_spec("Recovery Miss", 128, 64);
    pipeline.add_job(job_id, spec).await;

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }

    pipeline.hold_segments(&[held_segment(job_id, 2, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::missing(held_segment(job_id, 2, 0), "article not found").recovery(),
        )
        .await;

    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline
            .job_if_present(job_id)
            .map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn article_not_found_exhausts_when_the_work_already_failed_elsewhere() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40160);
    let spec = segmented_job_spec("Missing Everywhere", "gone.bin", &[128, 128, 128, 128]);
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[0, 0]);

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::missing(held_segment(job_id, 0, 0), "article not found")
                .on_server(1)
                .excluding(weaver_nntp::ServerSet::from([0])),
        )
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "both servers have now refused the article, so it is missing"
    );
    assert_eq!(pipeline.pending_retries(job_id), None);
    assert!(
        pipeline
            .job_if_present(job_id)
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
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(40161);
    let spec = segmented_job_spec("Nothing Learned", "loop.bin", &[128, 128, 128, 128]);
    pipeline.add_job(job_id, spec).await;
    pipeline.set_retention(&[0, 0, 0]);

    {
        let state = pipeline.job_mut(job_id);
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    }
    pipeline.hold_segments(&[held_segment(job_id, 0, 0)]);
    pipeline.open_pass(job_id);

    pipeline
        .failed_fetch(
            FailedFetch::missing(held_segment(job_id, 0, 0), "article not found")
                .on_server(1)
                .excluding(weaver_nntp::ServerSet::from([1])),
        )
        .await;

    assert_eq!(
        pipeline
            .metrics()
            .articles_not_found_without_new_server
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "a 430 that adds no server to the exclusion set must be counted"
    );
    assert_eq!(pipeline.pending_retries(job_id), None);
}
