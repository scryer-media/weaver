// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A lane that cannot be acquired must be visible, and must be retried.
//
// The acquire-failure handler keeps the download running either way — the
// work is requeued, or the leased articles take the failure and are retried
// through the ordinary result path — so the failure itself has to announce
// that it happened. Nothing above debug did, which made a job running on a
// fraction of its lanes look exactly like a slow server.

use super::*;

use crate::pipeline::download::transport::DownloadLaneMode;
use crate::pipeline::download::{
    DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL, DOWNLOAD_RETRY_STORM_THRESHOLD,
};

fn lease_for(pipeline: &mut Pipeline, job_id: JobId) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    let works = pipeline.dispatched_all(works);
    assert!(!works.is_empty(), "the fixture job has queued work");
    let completion_critical = works.iter().any(|work| work.completion_critical);
    pipeline.hold_test_lane_works(works.clone(), false);
    DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: 0,
        job_id,
        runtime_generation: 0,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        completion_critical,
        effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    }
}

// Selection contention hands the work straight back and asks for another
// dispatch pass. Without that wake the freed connection would sit idle until
// the run loop next came round on its own — with the work that needs it
// already queued.
#[tokio::test]
async fn a_contended_acquire_requeues_its_work_and_asks_for_another_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41501);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Contended Acquire", &many_standalone_files("contended", 4)),
    )
    .await;

    let lease = lease_for(&mut pipeline, job_id);
    let leased = lease.works.len();
    pipeline.active_download_connections = 1;
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    assert!(!pipeline.take_download_dispatch_wake());

    let mut pending = std::collections::VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended,
        },
        &mut pending,
    );

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        leased,
        "contention is not a verdict about the servers: every work item goes \
         back to the scheduler"
    );
    assert!(
        pipeline.take_download_dispatch_wake(),
        "and the freed connection is offered to the next pass immediately"
    );
    assert_eq!(
        pipeline.active_download_connections, 0,
        "the connection slot must not be left booked against a lane that never \
         opened"
    );
    assert!(
        pipeline
            .owned_lane_acquire_failure_log_throttle
            .last_emitted_at(job_id)
            .is_some(),
        "the failure must be reported rather than swallowed"
    );
}

// The warning is rate limited: a server that refuses one acquire refuses the
// next dispatch pass's too, and one line a minute is the budget.
#[tokio::test]
async fn repeated_acquire_failures_report_at_most_once_a_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41502);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Repeat Failures", &many_standalone_files("repeat", 4)),
    )
    .await;

    let mut pending = std::collections::VecDeque::new();
    let lease = lease_for(&mut pipeline, job_id);
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        },
        &mut pending,
    );
    let first = pipeline
        .owned_lane_acquire_failure_log_throttle
        .last_emitted_at(job_id)
        .expect("the first failure of a window reports itself");

    let lease = lease_for(&mut pipeline, job_id);
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        },
        &mut pending,
    );
    assert_eq!(
        pipeline
            .owned_lane_acquire_failure_log_throttle
            .last_emitted_at(job_id),
        Some(first),
        "the second failure inside the window is counted, not logged again"
    );
}

// The download lines that follow a state, as a test hears them: every event
// on this thread whose message names `prefix`, message first, then each field
// as `key=value`.
#[derive(Clone)]
struct StateLines {
    prefix: &'static str,
    heard: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StateLines {
    fn hear(prefix: &'static str) -> (Self, tracing::subscriber::DefaultGuard) {
        let lines = Self {
            prefix,
            heard: Arc::default(),
        };
        let guard = tracing::subscriber::set_default(lines.clone());
        (lines, guard)
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.heard.lock().unwrap())
    }
}

impl tracing::Subscriber for StateLines {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write;
        #[derive(Default)]
        struct Fields {
            message: String,
            fields: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    let _ = write!(self.message, "{value:?}");
                } else {
                    let _ = write!(self.fields, " {field}={value:?}");
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.message.starts_with(self.prefix) {
            self.heard.lock().unwrap().push(format!(
                "{} {}{}",
                event.metadata().level(),
                fields.message,
                fields.fields
            ));
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

// Lanes below their cap are only reported once they have stayed there, only
// while there is work they could be carrying, and only as the underfill
// starts and ends: a pass that finds it unchanged says nothing.
#[tokio::test]
async fn lanes_under_their_cap_are_reported_once_as_it_starts_and_once_as_it_ends() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41503);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Under Cap", &many_standalone_files("under-cap", 4)),
    )
    .await;
    let (lines, _guard) = StateLines::hear("download lanes");

    let start = Instant::now();
    pipeline.active_download_connections = 1;
    pipeline.last_owned_lane_acquire_failure_at = Some(start);
    pipeline.log_download_lanes_under_cap(start, 8);
    assert!(
        pipeline.download_lanes_under_cap_since.is_some(),
        "the window opens at the first pass that finds lanes unfilled"
    );
    assert!(
        lines.take().is_empty(),
        "a momentary gap between batches is not worth a line"
    );

    // Still under cap a window later, with a lane that failed to open inside
    // it: now it is worth one line, at debug, since the failed acquire is
    // warned about where it happens.
    let later = start + Duration::from_secs(6);
    pipeline.log_download_lanes_under_cap(later, 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with(
            "DEBUG download lanes have stayed below their connection cap with work queued"
        ),
        "{heard:?}"
    );
    assert!(pipeline.download_lanes_under_cap_reported);

    // However many passes find it unchanged, and however long it lasts,
    // nothing more is said while it lasts.
    for pass in 1..=1_000u64 {
        pipeline.last_owned_lane_acquire_failure_at = Some(later + Duration::from_secs(pass));
        pipeline.log_download_lanes_under_cap(later + Duration::from_secs(pass), 8);
    }
    assert!(
        lines.take().is_empty(),
        "an unchanged underfill is not news"
    );

    // Filling the lanes ends it, with one line.
    pipeline.active_download_connections = 8;
    let filled = later + Duration::from_secs(1_001);
    pipeline.log_download_lanes_under_cap(filled, 8);
    pipeline.log_download_lanes_under_cap(filled + Duration::from_secs(1), 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0]
            .starts_with("DEBUG download lanes are back at their connection cap or out of work"),
        "{heard:?}"
    );
    assert!(pipeline.download_lanes_under_cap_since.is_none());
    assert!(!pipeline.download_lanes_under_cap_reported);

    // An underfill with no lane failure behind it is one of the ordinary
    // kinds — a download quota, pressure, a job's tail — and stays quiet,
    // both as it starts and as it ends.
    pipeline.active_download_connections = 1;
    pipeline.last_owned_lane_acquire_failure_at = Some(start);
    let quiet = filled + Duration::from_secs(30);
    pipeline.log_download_lanes_under_cap(quiet, 8);
    pipeline.log_download_lanes_under_cap(quiet + Duration::from_secs(6), 8);
    assert!(!pipeline.download_lanes_under_cap_reported);
    pipeline.active_download_connections = 8;
    pipeline.log_download_lanes_under_cap(quiet + Duration::from_secs(7), 8);
    assert!(
        lines.take().is_empty(),
        "lanes below their cap with no failed acquire are not a fault"
    );

    // Running out of work ends a reported underfill too: idle lanes with an
    // empty queue are the ordinary end of a job.
    pipeline.active_download_connections = 0;
    let again = quiet + Duration::from_secs(60);
    pipeline.last_owned_lane_acquire_failure_at = Some(again);
    pipeline.log_download_lanes_under_cap(again, 8);
    pipeline.log_download_lanes_under_cap(again + Duration::from_secs(6), 8);
    assert_eq!(lines.take().len(), 1);
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.log_download_lanes_under_cap(again + Duration::from_secs(7), 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(heard[0].contains("dispatchable_queue_depth=0"), "{heard:?}");
    assert!(pipeline.download_lanes_under_cap_since.is_none());
}

// A retry storm is warned about once, when the first window finds it, and
// noted once when a later window finds articles completing again. The
// windows between say nothing, however many there are.
#[tokio::test]
async fn a_retry_storm_is_warned_once_as_it_starts_and_noted_once_as_it_ends() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (lines, _guard) = StateLines::hear("download retry storm");
    let window = DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    let retry = |pipeline: &Pipeline, count: u64| {
        pipeline
            .metrics
            .segments_retried
            .fetch_add(count, Ordering::Relaxed);
    };

    let t0 = Instant::now();
    pipeline.log_download_retry_storm(t0);
    // A window with retries under the threshold is not a storm.
    retry(&pipeline, DOWNLOAD_RETRY_STORM_THRESHOLD - 1);
    pipeline.log_download_retry_storm(t0 + window);
    assert!(lines.take().is_empty());
    assert!(pipeline.download_retry_storm.is_none());

    // The first window over the threshold with nothing finished starts it.
    retry(&pipeline, DOWNLOAD_RETRY_STORM_THRESHOLD);
    let started = t0 + window * 2;
    pipeline.log_download_retry_storm(started);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with("WARN download retry storm: articles are being retried"),
        "{heard:?}"
    );
    assert!(pipeline.download_retry_storm.is_some());

    // 600 more storming windows, as an overnight run would see, and calls
    // inside each window: nothing more is said.
    for step in 1..=600u32 {
        retry(&pipeline, DOWNLOAD_RETRY_STORM_THRESHOLD);
        pipeline.log_download_retry_storm(started + window * step - Duration::from_millis(1));
        pipeline.log_download_retry_storm(started + window * step);
    }
    assert!(lines.take().is_empty(), "a storm that goes on is not news");

    // A window in which an article completes ends it, with one line.
    pipeline
        .metrics
        .segments_downloaded
        .fetch_add(1, Ordering::Relaxed);
    let ended = started + window * 601;
    pipeline.log_download_retry_storm(ended);
    pipeline.log_download_retry_storm(ended + window);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with("INFO download retry storm over: articles are completing again"),
        "{heard:?}"
    );
    assert!(
        heard[0].contains(&format!(
            "retries_since_start={}",
            DOWNLOAD_RETRY_STORM_THRESHOLD * 600
        )),
        "{heard:?}"
    );
    assert!(pipeline.download_retry_storm.is_none());

    // A new storm after that is news again.
    retry(&pipeline, DOWNLOAD_RETRY_STORM_THRESHOLD);
    pipeline.log_download_retry_storm(ended + window * 2);
    assert_eq!(lines.take().len(), 1);
}

// Work whose only server is waiting on its recovery probe parks in its queue:
// no dispatch pass leases it to a lane that could only come back refused, so
// no refusal requeues it for the next pass to send again. The probe's answer
// is the one wake, and after it the server seats new work.
#[tokio::test]
async fn work_whose_server_waits_on_its_probe_parks_until_the_probe_answers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // The one server is a listener this test owns. It never answers, so a
    // lane seated after the probe's answer dials it and waits, here, on this
    // machine, for the rest of the test.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut server = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        tls: false,
        ..Default::default()
    });
    server.max_connections = 2;
    pipeline.nntp = std::sync::Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    pipeline.tuner.set_connection_limit(2);
    let job_id = JobId(41503);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Held Server", &many_standalone_files("held", 4)),
    )
    .await;
    let queued = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .len();
    assert!(queued > 0, "the fixture job has queued work");

    let probe = pipeline.nntp.pool().take_recovery_probe_for_test(0);
    let mut health = pipeline.nntp.pool().server_health_changes();
    for _ in 0..3 {
        pipeline.dispatch_downloads();
    }

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        queued,
        "no pass leases work to a server whose new connections are held"
    );
    assert_eq!(pipeline.active_download_connections, 0);
    assert!(pipeline.download_lane_owners.is_empty());

    probe.answer();
    health.changed().await;
    assert_eq!(pipeline.nntp.pool().connection_hold(0, true), None);

    pipeline.dispatch_downloads();
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
            < queued,
        "the answered server seats the waiting work"
    );
    drop(listener);
}
