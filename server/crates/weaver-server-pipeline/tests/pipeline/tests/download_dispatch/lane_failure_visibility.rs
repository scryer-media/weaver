// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A lane that cannot be acquired must be visible, and must be retried.
//
// The acquire-failure handler keeps the download running either way — the
// work is requeued, or the leased articles take the failure and are retried
// through the ordinary result path — so the failure itself has to announce
// that it happened. Nothing above debug did, which made a job running on a
// fraction of its lanes look exactly like a slow server.

use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use weaver_nntp::client::BlockingBodyLaneAcquireError;
use weaver_server_composition::{JobId, JobStatus};
use weaver_server_pipeline::DownloadQueue;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::{
    DispatchFixture,
    acquire_failures::{DISPATCH_STALL_LOG_INTERVAL, RETRY_STORM_THRESHOLD, RoutePress},
};

fn queued_works(pipeline: &DispatchFixture, job_id: JobId) -> usize {
    pipeline
        .job(job_id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

// Selection contention hands the work straight back and asks for another
// dispatch pass. Without that wake the freed connection would sit idle until
// the run loop next came round on its own — with the work that needs it
// already queued.
#[tokio::test]
async fn a_contended_acquire_requeues_its_work_and_asks_for_another_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41501);
    pipeline
        .add_files(job_id, "Contended Acquire", "contended", 4)
        .await;

    let lease = pipeline.acquire_lease(job_id);
    let leased = lease.works().len();
    pipeline.set_active_connections(1);
    pipeline.set_job_active_connections(job_id, 1);
    assert!(!pipeline.take_dispatch_wake());

    let _pending = pipeline.refuse_acquire(lease, BlockingBodyLaneAcquireError::SelectionContended);

    assert_eq!(
        queued_works(&pipeline, job_id),
        leased,
        "contention is not a verdict about the servers: every work item goes \
         back to the scheduler"
    );
    assert!(
        pipeline.take_dispatch_wake(),
        "and the freed connection is offered to the next pass immediately"
    );
    assert_eq!(
        pipeline.active_connections(),
        0,
        "the connection slot must not be left booked against a lane that never \
         opened"
    );
    assert!(
        pipeline.last_refusal_log(job_id).is_some(),
        "the failure must be reported rather than swallowed"
    );
}

// The warning is rate limited: a server that refuses one acquire refuses the
// next dispatch pass's too, and one line a minute is the budget.
#[tokio::test]
async fn repeated_acquire_failures_report_at_most_once_a_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41502);
    pipeline
        .add_files(job_id, "Repeat Failures", "repeat", 4)
        .await;

    let lease = pipeline.acquire_lease(job_id);
    let _pending = pipeline.refuse_acquire(lease, BlockingBodyLaneAcquireError::LocalCapacity);
    let first = pipeline
        .last_refusal_log(job_id)
        .expect("the first failure of a window reports itself");

    let lease = pipeline.acquire_lease(job_id);
    let _pending = pipeline.refuse_acquire(lease, BlockingBodyLaneAcquireError::LocalCapacity);
    assert_eq!(
        pipeline.last_refusal_log(job_id),
        Some(first),
        "the second failure inside the window is counted, not logged again"
    );
}

// A lane refused because the server's network route is full (a tunnel at
// capacity, or its session budget spent) is a wait, not a fault: the lane
// parks as capacity, and every leased article waits the lane-unavailable
// delay with its retry budget untouched and nothing failed. It does not come
// straight back, since nothing tells the scheduler when the route frees.
#[tokio::test]
async fn a_full_route_parks_as_capacity_and_paces_its_work_without_failing_it() {
    for refusal in [
        weaver_tunnel::pipe::DialError::AtCapacity(Default::default()),
        weaver_tunnel::pipe::DialError::SessionBudget,
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::new(&temp_dir).await;
        let job_id = JobId(41504);
        pipeline
            .add_files(job_id, "Full Route", "full-route", 4)
            .await;

        let lease = pipeline.acquire_lease(job_id);
        let leased: Vec<_> = lease.works().iter().map(|work| work.segment_id).collect();
        let retry_counts: Vec<_> = lease.works().iter().map(|work| work.retry_count).collect();
        pipeline.set_active_connections(1);
        pipeline.set_job_active_connections(job_id, 1);
        let parks_error_before = pipeline
            .metrics()
            .download_lane_parks_error_total
            .load(Ordering::Relaxed);
        // No counter counts a capacity park, so the park is heard as its
        // line, which names the reason.
        let (parks, parks_guard) = StateLines::hear("download lane parked");

        let pending = pipeline.refuse_acquire(
            lease,
            BlockingBodyLaneAcquireError::RouteCapacity(weaver_nntp::NntpError::Route(Arc::new(
                refusal,
            ))),
        );

        assert_eq!(
            queued_works(&pipeline, job_id),
            0,
            "a full route must not hand its work straight back to redial it"
        );
        assert_eq!(pending.len(), leased.len());
        assert_eq!(
            pipeline
                .metrics()
                .download_lane_parks_error_total
                .load(Ordering::Relaxed),
            parks_error_before,
            "the lane parks as capacity, not as an error"
        );
        let parked = parks.take();
        drop(parks_guard);
        assert_eq!(parked.len(), 1, "{parked:?}");
        assert!(parked[0].contains(" reason=Capacity"), "{parked:?}");
        assert!(
            parked[0].contains(" released_connection=true"),
            "{parked:?}"
        );
        assert_eq!(pipeline.active_connections(), 0);

        let watermark = tokio::time::Instant::now();
        pipeline.hear_refused_acquires(pending).await;

        let state = pipeline.job(job_id);
        assert_eq!(state.failed_bytes, 0);
        assert_eq!(
            pipeline
                .metrics()
                .segments_failed_permanent
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            pipeline.wait_reason(job_id),
            Some("NNTP server's network route is full")
        );
        assert_eq!(pipeline.parked_retry_count(), leased.len());
        for (deadline, retry) in pipeline.parked_works_with_deadlines() {
            let index = leased
                .iter()
                .position(|segment| *segment == retry.segment_id)
                .expect("only the leased articles wait");
            assert_eq!(
                retry.retry_count, retry_counts[index],
                "waiting for the route spends no retry"
            );
            let deadline = deadline.expect("the wait is paced, not open-ended");
            assert!(
                deadline >= watermark + std::time::Duration::from_millis(250),
                "the work waits the lane-unavailable delay before redialling"
            );
        }
    }
}

// Every paced retry of the job back on its queue, as their delay ending
// puts them.
fn requeue_every_retry(pipeline: &mut DispatchFixture) {
    let later = tokio::time::Instant::now() + Duration::from_secs(3600);
    for retry in pipeline.take_due_retries(later) {
        pipeline.requeue_retry(retry);
    }
}

// A lane refused for a full route presses while its work waits to be handed
// out again, through a pass that runs before its refusal is heard and while
// the work waits its paced retry. Once the work is handed to another lane
// and a pass finds nothing left to hand out, the press is over, with no wait
// for the idle lane's release.
#[tokio::test]
async fn a_route_press_ends_once_its_work_went_to_another_lane_and_none_waits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41521);
    pipeline
        .add_files(job_id, "Press Ends", "press-ends", 4)
        .await;

    let lease = pipeline.acquire_counted_lease(job_id);
    let press = pipeline.report_route_press(&lease);
    pipeline.dispatch();
    assert!(
        press.stands(),
        "a pass before the refusal is heard sees the work in flight, not gone"
    );
    pipeline.refuse_full_route(lease).await;
    pipeline.dispatch();
    assert!(press.stands(), "the work waits its paced retry");

    requeue_every_retry(&mut pipeline);
    pipeline.publish_download_drained();
    assert!(press.stands(), "the work waits on the queue");
    // Another lane, holding a connection, takes it.
    let _elsewhere = pipeline.acquire_counted_lease(job_id);
    pipeline.dispatch();
    assert!(
        !press.stands(),
        "the work went to another lane and nothing else waits"
    );
}

// A job that has moved on past its download lets every press it left go:
// each lane's refused dial is heard, and the pass that finds the job with
// nothing to hand out ends them all.
#[tokio::test]
async fn a_job_moved_past_its_download_lets_every_route_press_go() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41522);
    pipeline.add_files(job_id, "Moved On", "moved-on", 4).await;

    let mut presses = Vec::new();
    for _ in 0..2 {
        let lease = pipeline.acquire_counted_lease(job_id);
        presses.push(pipeline.report_route_press(&lease));
        pipeline.refuse_full_route(lease).await;
        requeue_every_retry(&mut pipeline);
    }
    let last = pipeline.acquire_counted_lease(job_id);
    pipeline.refuse_full_route(last).await;
    pipeline.dispatch();
    assert!(presses.iter().all(RoutePress::stands));

    // The job's download ends: what it waited on is settled, and it moves
    // on to its verification.
    let _ = pipeline
        .take_due_retries(tokio::time::Instant::now() + std::time::Duration::from_secs(3600));
    pipeline.clear_pending_retries(job_id);
    pipeline.job_mut(job_id).status = JobStatus::Verifying;
    pipeline.dispatch();
    assert!(
        presses.iter().all(|press| !press.stands()),
        "no press outlives the job's download"
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
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41503);
    pipeline
        .add_files(job_id, "Under Cap", "under-cap", 4)
        .await;
    let (lines, _guard) = StateLines::hear("download lanes");

    let start = Instant::now();
    pipeline.set_active_connections(1);
    pipeline.record_acquire_failure_time(start);
    pipeline.log_lanes_under_cap(start, 8);
    assert!(
        pipeline.lanes_under_cap_since().is_some(),
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
    pipeline.log_lanes_under_cap(later, 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with(
            "DEBUG download lanes have stayed below their connection cap with work queued"
        ),
        "{heard:?}"
    );
    assert!(pipeline.lanes_under_cap_reported());

    // However many passes find it unchanged, and however long it lasts,
    // nothing more is said while it lasts.
    for pass in 1..=1_000u64 {
        pipeline.record_acquire_failure_time(later + Duration::from_secs(pass));
        pipeline.log_lanes_under_cap(later + Duration::from_secs(pass), 8);
    }
    assert!(
        lines.take().is_empty(),
        "an unchanged underfill is not news"
    );

    // Filling the lanes ends it, with one line.
    pipeline.set_active_connections(8);
    let filled = later + Duration::from_secs(1_001);
    pipeline.log_lanes_under_cap(filled, 8);
    pipeline.log_lanes_under_cap(filled + Duration::from_secs(1), 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0]
            .starts_with("DEBUG download lanes are back at their connection cap or out of work"),
        "{heard:?}"
    );
    assert!(pipeline.lanes_under_cap_since().is_none());
    assert!(!pipeline.lanes_under_cap_reported());

    // An underfill with no lane failure behind it is one of the ordinary
    // kinds — a download quota, pressure, a job's tail — and stays quiet,
    // both as it starts and as it ends.
    pipeline.set_active_connections(1);
    pipeline.record_acquire_failure_time(start);
    let quiet = filled + Duration::from_secs(30);
    pipeline.log_lanes_under_cap(quiet, 8);
    pipeline.log_lanes_under_cap(quiet + Duration::from_secs(6), 8);
    assert!(!pipeline.lanes_under_cap_reported());
    pipeline.set_active_connections(8);
    pipeline.log_lanes_under_cap(quiet + Duration::from_secs(7), 8);
    assert!(
        lines.take().is_empty(),
        "lanes below their cap with no failed acquire are not a fault"
    );

    // Running out of work ends a reported underfill too: idle lanes with an
    // empty queue are the ordinary end of a job.
    pipeline.set_active_connections(0);
    let again = quiet + Duration::from_secs(60);
    pipeline.record_acquire_failure_time(again);
    pipeline.log_lanes_under_cap(again, 8);
    pipeline.log_lanes_under_cap(again + Duration::from_secs(6), 8);
    assert_eq!(lines.take().len(), 1);
    pipeline
        .job_mut(job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.log_lanes_under_cap(again + Duration::from_secs(7), 8);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(heard[0].contains("dispatchable_queue_depth=0"), "{heard:?}");
    assert!(pipeline.lanes_under_cap_since().is_none());
}

// A retry storm is warned about once, when the first window finds it, and
// noted once when a later window finds articles completing again. The
// windows between say nothing, however many there are.
#[tokio::test]
async fn a_retry_storm_is_warned_once_as_it_starts_and_noted_once_as_it_ends() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let (lines, _guard) = StateLines::hear("download retry storm");
    let window = DISPATCH_STALL_LOG_INTERVAL;
    let retry = |pipeline: &DispatchFixture, count: u64| {
        pipeline
            .metrics()
            .segments_retried
            .fetch_add(count, Ordering::Relaxed);
    };

    let t0 = Instant::now();
    pipeline.log_retry_storm(t0);
    // A window with retries under the threshold is not a storm.
    retry(&pipeline, RETRY_STORM_THRESHOLD - 1);
    pipeline.log_retry_storm(t0 + window);
    assert!(lines.take().is_empty());
    assert!(!pipeline.retry_storm_active());

    // The first window over the threshold with nothing finished starts it.
    retry(&pipeline, RETRY_STORM_THRESHOLD);
    let started = t0 + window * 2;
    pipeline.log_retry_storm(started);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with("WARN download retry storm: articles are being retried"),
        "{heard:?}"
    );
    assert!(pipeline.retry_storm_active());

    // 600 more storming windows, as an overnight run would see, and calls
    // inside each window: nothing more is said.
    for step in 1..=600u32 {
        retry(&pipeline, RETRY_STORM_THRESHOLD);
        pipeline.log_retry_storm(started + window * step - Duration::from_millis(1));
        pipeline.log_retry_storm(started + window * step);
    }
    assert!(lines.take().is_empty(), "a storm that goes on is not news");

    // A window in which an article completes ends it, with one line.
    pipeline
        .metrics()
        .segments_downloaded
        .fetch_add(1, Ordering::Relaxed);
    let ended = started + window * 601;
    pipeline.log_retry_storm(ended);
    pipeline.log_retry_storm(ended + window);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].starts_with("INFO download retry storm over: articles are completing again"),
        "{heard:?}"
    );
    assert!(
        heard[0].contains(&format!(
            "retries_since_start={}",
            RETRY_STORM_THRESHOLD * 600
        )),
        "{heard:?}"
    );
    assert!(!pipeline.retry_storm_active());

    // A new storm after that is news again.
    retry(&pipeline, RETRY_STORM_THRESHOLD);
    pipeline.log_retry_storm(ended + window * 2);
    assert_eq!(lines.take().len(), 1);
}

// Work whose only server is waiting on its recovery probe parks in its queue:
// no dispatch pass leases it to a lane that could only come back refused, so
// no refusal requeues it for the next pass to send again. The probe's answer
// is the one wake, and after it the server seats new work.
#[tokio::test]
async fn work_whose_server_waits_on_its_probe_parks_until_the_probe_answers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
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
    pipeline.set_client(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    pipeline.set_connection_limit(2);
    let job_id = JobId(41503);
    pipeline.add_files(job_id, "Held Server", "held", 4).await;
    let queued = queued_works(&pipeline, job_id);
    assert!(queued > 0, "the fixture job has queued work");

    let probe = pipeline.client().pool().take_recovery_probe_for_test(0);
    let mut health = pipeline.client().pool().server_health_changes();
    for _ in 0..3 {
        pipeline.dispatch();
    }

    assert_eq!(
        queued_works(&pipeline, job_id),
        queued,
        "no pass leases work to a server whose new connections are held"
    );
    assert_eq!(pipeline.active_connections(), 0);
    assert!(pipeline.lane_owners_empty());

    probe.answer();
    health.changed().await;
    assert_eq!(pipeline.client().pool().connection_hold(0, true), None);

    pipeline.dispatch();
    assert!(
        queued_works(&pipeline, job_id) < queued,
        "the answered server seats the waiting work"
    );
    drop(listener);
}
