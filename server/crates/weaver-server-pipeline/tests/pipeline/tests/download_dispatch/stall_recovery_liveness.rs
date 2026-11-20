// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::stall_liveness_fixture as liveness;

// The liveness stall lines a test hears: every WARN event on this thread
// whose message is the liveness stall's, as `message key=value...`.
#[derive(Clone, Default)]
struct LivenessLines(Arc<std::sync::Mutex<Vec<String>>>);

impl LivenessLines {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl tracing::Subscriber for LivenessLines {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
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
        if fields
            .message
            .starts_with("download dispatch liveness stall")
        {
            self.0
                .lock()
                .unwrap()
                .push(format!("{}{}", fields.message, fields.fields));
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!(" {key}=");
    let start = line.find(&prefix)? + prefix.len();
    line[start..].split(' ').next()
}

// The liveness stall counts only work dispatch can hand out. A verifying
// job ahead in the queue with its recovery parked is not stalled work: the
// line names the job whose queued downloads are not moving and counts only
// those. Once a seek round promotes the verifying job's recovery into its
// download queue, that recovery is work dispatch can hand out, and the line
// names it.
#[tokio::test]
async fn the_liveness_stall_counts_promoted_recovery_and_not_parked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    // Only the line is under test; the watchdog's escalations stay out.
    set_recovery(&mut pipeline, false);
    let verifying = JobId(40010);
    let downloading = JobId(40011);
    insert_stalled_job(&mut pipeline, verifying, 4).await;
    insert_stalled_job(&mut pipeline, downloading, 2).await;
    {
        let state = job_mut(&mut pipeline, verifying);
        let active = state.articles.active_mut().unwrap();
        for work in active.download_queue.drain_all() {
            active.recovery_queue.push(work);
        }
        state.status = JobStatus::Verifying;
    }
    assert_eq!(
        liveness::job_position(&pipeline, verifying),
        Some(0),
        "the verifying job is the first the line could pick"
    );
    assert!(liveness::admitted(&pipeline, verifying));
    assert!(liveness::admitted(&pipeline, downloading));

    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    dispatch_at(&mut pipeline, t0);
    assert!(
        lines.take().is_empty(),
        "the first pass only starts the clock"
    );
    dispatch_at(&mut pipeline, t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    assert_eq!(liveness::articles_in_flight(&pipeline), 0);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40011"), "{}", heard[0]);
    assert_eq!(
        field(&heard[0], "dispatchable_queue_depth"),
        Some("2"),
        "{}",
        heard[0]
    );

    // A seek round promotes the parked recovery back to the download queue.
    {
        let active = job_mut(&mut pipeline, verifying)
            .articles
            .active_mut()
            .unwrap();
        for work in active.recovery_queue.drain_all() {
            active.download_queue.push(work);
        }
    }
    let t1 = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, t1);
    assert_eq!(liveness::articles_in_flight(&pipeline), 0);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40010"), "{}", heard[0]);
    assert_eq!(field(&heard[0], "parked_recovery_len"), Some("0"));
    assert_eq!(field(&heard[0], "download_queue_len"), Some("4"));
    assert_eq!(field(&heard[0], "dispatchable_queue_depth"), Some("6"));
    assert_eq!(last_log(&pipeline), Some(t1));
}

// The passes right after a job is added or a restart installs its jobs open
// nothing for a moment. That says nothing; a warning comes only once the
// passes have opened nothing for the whole log interval. The slow-disk run
// warned 0.1 ms after its restart installed the jobs, and downloaded three
// seconds later.
#[tokio::test]
async fn passes_that_open_nothing_warn_only_once_they_have_lasted_the_log_interval() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    set_recovery(&mut pipeline, false);
    let job_id = JobId(40020);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();

    dispatch_at(&mut pipeline, t0);
    let almost = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_millis(1);
    dispatch_at(&mut pipeline, almost);
    assert_eq!(lines.take(), Vec::<String>::new());
    assert_eq!(liveness_since(&pipeline), Some(t0));

    let lasted = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    dispatch_at(&mut pipeline, lasted);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40020"), "{}", heard[0]);
}

// A job its volume holds at the hard limit waits on the disk. However long
// that lasts, it is not warned of as a dispatch stall.
#[tokio::test]
async fn a_job_held_by_its_volume_is_not_warned_of_as_a_stall() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40021);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let volume = working_volume(&pipeline, job_id);
    let budget = volume_lease_limit(&volume);
    volume.count_for_test(budget);
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();

    for at in [
        t0,
        t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL,
        t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER,
    ] {
        dispatch_at(&mut pipeline, at);
    }
    assert_eq!(lines.take(), Vec::<String>::new());

    // Below the limit the same passes are a stall again, and warned of.
    volume.count_for_test(budget - 1);
    let later = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    dispatch_at(&mut pipeline, later);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40021"), "{}", heard[0]);
}

// Queued work with nothing in flight is reported only once it has held for a
// whole interval. One job ending and the next starting looks like that for a
// moment; a pass with work in flight in between starts the clock over.
#[tokio::test]
async fn the_liveness_stall_speaks_only_after_a_whole_interval() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    set_recovery(&mut pipeline, false);
    let job_id = JobId(40012);
    insert_stalled_job(&mut pipeline, job_id, 2).await;

    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    dispatch_at(&mut pipeline, t0);
    assert!(lines.take().is_empty(), "the first pass says nothing");
    assert_eq!(liveness_since(&pipeline), Some(t0));

    // A pass with an article in flight clears the clock. The article is
    // only counted, held by no lane, so the lane audit stays off for it.
    let busy_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_secs(1);
    liveness::busy_dispatch_at(&mut pipeline, busy_at);
    assert!(lines.take().is_empty());
    assert_eq!(liveness_since(&pipeline), None);

    // The interval since the first pass has passed, but not since the
    // condition came back.
    let back_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    dispatch_at(&mut pipeline, back_at);
    assert!(lines.take().is_empty(), "the clock started over");
    assert_eq!(liveness_since(&pipeline), Some(back_at));

    dispatch_at(
        &mut pipeline,
        back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL,
    );
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40012"), "{}", heard[0]);
}

// The first pass of a stalled job, with the liveness lines it writes captured.
async fn liveness_clock_started(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
) -> (
    Pipeline,
    LivenessLines,
    tracing::subscriber::DefaultGuard,
    Instant,
) {
    let mut pipeline = stalled_pipeline(temp_dir).await;
    set_recovery(&mut pipeline, false);
    insert_stalled_job(&mut pipeline, job_id, 2).await;
    let lines = LivenessLines::default();
    let capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    dispatch_at(&mut pipeline, t0);
    assert!(lines.take().is_empty());
    assert_eq!(liveness_since(&pipeline), Some(t0));
    (pipeline, lines, capture, t0)
}

// After a spell that cleared the clock, the first pass that finds queued
// work and nothing in flight starts it again and says nothing, however long
// ago the first stall began; a whole interval later the line is written.
fn assert_the_liveness_clock_started_over(
    pipeline: &mut Pipeline,
    lines: &LivenessLines,
    t0: Instant,
    job: &str,
) {
    assert!(lines.take().is_empty());
    assert_eq!(liveness_since(pipeline), None);
    let back_at = t0 + Duration::from_secs(60 * 60);
    dispatch_at(pipeline, back_at);
    assert!(lines.take().is_empty(), "the clock started over");
    assert_eq!(liveness_since(pipeline), Some(back_at));

    let inside = back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_secs(1);
    dispatch_at(pipeline, inside);
    assert!(lines.take().is_empty());

    dispatch_at(pipeline, back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some(job), "{}", heard[0]);
}

// A connection handoff means dispatch is stopped on purpose, so the stall
// timed before it is over: the first pass after it is not taken for a stall
// that has held all along.
#[tokio::test]
async fn a_handoff_starts_the_liveness_clock_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, JobId(40013)).await;

    liveness::handoff(&mut pipeline, true);
    dispatch_at(&mut pipeline, t0 + Duration::from_secs(60));
    liveness::handoff(&mut pipeline, false);
    assert_the_liveness_clock_started_over(&mut pipeline, &lines, t0, "40013");
}

// So does an operator pause.
#[tokio::test]
async fn an_operator_pause_starts_the_liveness_clock_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, JobId(40014)).await;

    liveness::set_manual_pause(&pipeline, true);
    dispatch_at(&mut pipeline, t0 + Duration::from_secs(60));
    liveness::set_manual_pause(&pipeline, false);
    assert_the_liveness_clock_started_over(&mut pipeline, &lines, t0, "40014");
}

// And so does a pass that finds nothing dispatch would serve.
#[tokio::test]
async fn a_pass_with_no_queued_work_starts_the_liveness_clock_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40015);
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, job_id).await;

    let parked: Vec<_> = job_mut(&mut pipeline, job_id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all()
        .into_iter()
        .collect();
    dispatch_at(&mut pipeline, t0 + Duration::from_secs(60));
    for work in parked {
        job_mut(&mut pipeline, job_id)
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
    assert_the_liveness_clock_started_over(&mut pipeline, &lines, t0, "40015");
}

// The rate limit only holds a pass back; it does not show the stall over.
// The clock runs through it, and the first pass that gets as far as the work
// after a whole interval writes the line.
#[tokio::test]
async fn a_rate_limit_wait_leaves_the_liveness_clock_running() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, JobId(40016)).await;

    // A debt at one byte a second that no test run outlasts.
    liveness::set_rate(&mut pipeline, Some(1));
    liveness::consume_rate(&mut pipeline, u64::from(u32::MAX));
    assert!(liveness::rate_wait(&mut pipeline));
    dispatch_at(&mut pipeline, t0 + Duration::from_secs(60));
    assert!(lines.take().is_empty());
    assert_eq!(liveness_since(&pipeline), Some(t0));

    liveness::set_rate(&mut pipeline, None);
    assert!(!liveness::rate_wait(&mut pipeline));
    dispatch_at(&mut pipeline, t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40016"), "{}", heard[0]);
}

// Soft byte pressure spaces passes out; a pass it turns away leaves the
// clock running, and a stall that holds under soft pressure is still
// reported after a whole interval.
#[tokio::test]
async fn a_stall_under_soft_byte_pressure_is_still_reported() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, JobId(40017)).await;

    liveness::soft_decode_pressure(&mut pipeline, 1000, 700);
    let turned_away_at = t0 + Duration::from_secs(60);
    liveness::set_soft_dispatch_after(
        &mut pipeline,
        Some(turned_away_at + Duration::from_secs(60 * 60)),
    );
    dispatch_at(&mut pipeline, turned_away_at);
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_state
            .load(Ordering::Relaxed),
        weaver_server_pipeline::DownloadPressureState::Soft.as_code()
    );
    assert_eq!(
        liveness::soft_dispatch_after(&pipeline),
        Some(turned_away_at + Duration::from_secs(60 * 60)),
        "the pass was turned away"
    );
    assert!(lines.take().is_empty());
    assert_eq!(liveness_since(&pipeline), Some(t0));

    // Still under soft pressure, a pass that is let through writes the line.
    liveness::set_soft_dispatch_after(&mut pipeline, None);
    dispatch_at(&mut pipeline, t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_state
            .load(Ordering::Relaxed),
        weaver_server_pipeline::DownloadPressureState::Soft.as_code()
    );
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40017"), "{}", heard[0]);
}

// A stalled job whose queued articles server 0 lacks, leaving server 1, which
// is quarantined for transport, as the only server for them. Each minute,
// as the pool's recovery backoff (60 s at most) allows, server 1's probe
// comes due, takes the job's highest-priority article from the scheduler,
// fails with `kind`, and the server is quarantined again. Server 0 stays
// healthy, so a server is always there to dispatch to. Time is the paused
// tokio clock, and a pass runs at each instant the loop would wake for: a
// probe's failure handled, and each parked retry coming due. Stops at the
// release or after twenty minutes, past both windows. Returns the job's
// articles booked missing and a line per pass on what the watch did.
async fn a_probe_fails_every_minute(
    pipeline: &mut Pipeline,
    job: JobId,
    kind: liveness::ProbeFailure,
) -> (u64, Vec<String>) {
    insert_stalled_job(pipeline, job, 4).await;
    let queue = &mut job_mut(pipeline, job)
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut works = queue.drain_all();
    for work in &mut works {
        work.exclude_servers = weaver_nntp::ServerSet::from([0]);
    }
    for work in works {
        queue.push(work);
    }
    let pool = Arc::clone(client_of(pipeline).pool());
    pool.quarantine_for_transport_for_test(1);
    let not_found_before = metrics(pipeline).articles_not_found.load(Ordering::Relaxed);

    let t0 = tokio::time::Instant::now();
    let rel = |at: Instant| -> String {
        let t0 = t0.into_std();
        if at >= t0 {
            format!("{}s", (at - t0).as_secs_f64())
        } else {
            format!("-{}s", (t0 - at).as_secs_f64())
        }
    };
    let end = t0 + Duration::from_secs(20 * 60);
    let mut next_probe = t0;
    let mut trace = Vec::new();
    loop {
        let next_retry = liveness::retry_deadline(pipeline);
        let next = next_retry.map_or(next_probe, |retry| retry.min(next_probe));
        if next > end {
            break;
        }
        tokio::time::advance(next.saturating_duration_since(tokio::time::Instant::now())).await;
        let now = tokio::time::Instant::now();
        let mut event = Vec::new();
        let due = liveness::requeue_due_retries(pipeline, now);
        if due != 0 {
            event.push(format!("{due} retries due"));
        }
        if now >= next_probe {
            next_probe += Duration::from_secs(60);
            pool.expire_quarantine_for_test(1);
            let works = liveness::probe_handout(pipeline, 1);
            let probe = pool.take_recovery_probe_for_test(1);
            for work in works {
                event.push(format!("probe fails {}", work.segment_id.segment_number));
                liveness::reject_probe(pipeline, work, kind).await;
            }
            drop(probe);
            pool.quarantine_for_transport_for_test(1);
        }
        dispatch_at(pipeline, now.into_std());
        let parked: Vec<String> = liveness::parked_deadlines(pipeline)
            .map(|(deadline, retry)| {
                format!(
                    "{}@{}",
                    retry.segment_id.segment_number,
                    deadline.map_or("none".into(), |at| rel(at.into_std()))
                )
            })
            .collect();
        let watch = stall_since(pipeline).map(|since| {
            format!(
                "since {} kicked {:?}",
                rel(since),
                kick(pipeline).map(|(at, _)| rel(*at))
            )
        });
        trace.push(format!(
            "t={} [{}] watch={:?} explained={:?} parked={parked:?} servers={:?} kicks={} releases={}",
            rel(now.into_std()),
            event.join(", "),
            watch,
            earliest(pipeline, now.into_std())
                .map(rel),
            client_of(pipeline).blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
                .map(|order| order.len()),
            kicks(pipeline),
            releases(pipeline),
        ));
        if releases(pipeline) > 0 {
            break;
        }
    }
    (
        metrics(pipeline).articles_not_found.load(Ordering::Relaxed) - not_found_before,
        trace,
    )
}

// The probe fails on an established connection, so an article whose probe
// keeps failing has its retry held past the recovery backoff. The comment on
// the watchdog's outage branch promises that a stalled job whose articles
// only a cooled-down server holds still escalates while another server is
// available: kicked, then released with its queued articles booked missing.
#[tokio::test(start_paused = true)]
async fn a_job_only_a_quarantined_server_could_serve_escalates_while_its_probe_fails_on_the_connection()
 {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41940);
    let (booked, trace) = a_probe_fails_every_minute(
        &mut pipeline,
        job,
        liveness::ProbeFailure::ESTABLISHED_TRANSPORT,
    )
    .await;
    let trace = trace.join("\n");
    assert_eq!(kicks(&pipeline), 1, "kicked once:\n{trace}");
    assert_eq!(releases(&pipeline), 1, "released:\n{trace}");
    assert!(booked > 0, "queued articles booked missing:\n{trace}");
    assert_eq!(
        stall_job(&pipeline, job)
            .articles
            .active()
            .map_or(0, |active| active.download_queue.len()),
        0,
        "{trace}"
    );
}

// As above, with the probe failing at the dial: its article's retry waits
// one second.
#[tokio::test(start_paused = true)]
async fn a_job_only_a_quarantined_server_could_serve_escalates_while_its_probe_fails_to_dial() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41941);
    let (booked, trace) = a_probe_fails_every_minute(
        &mut pipeline,
        job,
        liveness::ProbeFailure::CONNECTION_ESTABLISHMENT,
    )
    .await;
    let trace = trace.join("\n");
    assert_eq!(kicks(&pipeline), 1, "kicked once:\n{trace}");
    assert_eq!(releases(&pipeline), 1, "released:\n{trace}");
    assert!(booked > 0, "queued articles booked missing:\n{trace}");
    assert_eq!(
        stall_job(&pipeline, job)
            .articles
            .active()
            .map_or(0, |active| active.download_queue.len()),
        0,
        "{trace}"
    );
}

// A timed infrastructure retry of one of a stalled job's articles explains
// the job's stall until its deadline; the same retry held for a quarantined
// server's probe explains nothing.
#[tokio::test]
async fn a_timed_retry_explains_its_jobs_stall_and_a_probe_hold_does_not() {
    for (hold, explains) in [
        (liveness::RetryWait::Ordinary, true),
        (liveness::RetryWait::Probe, false),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = stalled_pipeline(&temp_dir).await;
        let job_id = JobId(40020);
        insert_stalled_job(&mut pipeline, job_id, 3).await;
        liveness::park_timed_retry(&mut pipeline, job_id, hold, Duration::from_secs(90));
        let deadline = liveness::retry_deadline(&pipeline)
            .expect("the retry is parked with a deadline")
            .into_std();
        let now = Instant::now();
        assert!(deadline > now);
        assert_eq!(
            earliest(&pipeline, now),
            explains.then_some(deadline),
            "{hold:?}"
        );
    }
}

fn queued(pipeline: &Pipeline, job_id: JobId) -> usize {
    stall_job(pipeline, job_id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

fn not_found(pipeline: &Pipeline) -> u64 {
    metrics(pipeline).articles_not_found.load(Ordering::Relaxed)
}

// A health probe round answering on the lanes it was yielded holds every
// stalled job's dispatch on purpose, its own job's and the job beside it.
// Here it answers batch after batch, two on the lanes, one seated every
// forty seconds, for longer than the kick and release windows together, as
// a large post's round does on two connections. Nothing is logged as a
// stall, nothing is kicked, and no queued article of either job is booked
// missing. Before the probe explained the wait, this round was kicked at
// the kick window and both jobs were released at the release window.
#[tokio::test]
async fn a_probe_round_answering_on_the_lanes_is_never_reported_kicked_or_released() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    assert!(recovery(&pipeline));
    let probing = JobId(40030);
    let beside = JobId(40031);
    insert_stalled_job(&mut pipeline, probing, 3).await;
    insert_stalled_job(&mut pipeline, beside, 2).await;
    let not_found_before = not_found(&pipeline);
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());

    let t0 = Instant::now();
    let round_ends = t0
        + DOWNLOAD_DISPATCH_STALL_KICK_AFTER
        + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER
        + 2 * DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    let mut at = t0;
    while at <= round_ends {
        liveness::seat_probe_round(&mut pipeline, probing, 2, at);
        dispatch_at(&mut pipeline, at);
        at += Duration::from_secs(40);
    }

    // Booked missing first, the bug this guards; the rest after.
    assert_eq!(
        not_found(&pipeline),
        not_found_before,
        "no article booked missing"
    );
    assert_eq!(queued(&pipeline, probing), 3);
    assert_eq!(queued(&pipeline, beside), 2);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(kicks(&pipeline), 0);
    assert!(lines.take().is_empty(), "no stall is reported");
    assert_eq!(stall_job(&pipeline, probing).failed_bytes, 0);
    assert_eq!(stall_job(&pipeline, beside).failed_bytes, 0);
    assert!(!completion_pending(&pipeline, probing));
    assert!(!completion_pending(&pipeline, beside));
}

// A round whose batches wait for a seat and have none on the lanes runs no
// probe: the stall is logged after a whole interval, kicked at the kick
// window and released at the release window, as any stall nothing explains.
#[tokio::test]
async fn a_round_with_no_batch_on_the_lanes_explains_no_stall() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40032);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let not_found_before = not_found(&pipeline);
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());

    let t0 = Instant::now();
    liveness::seat_probe_round(&mut pipeline, job_id, 0, t0);
    assert!(!liveness::probe_answering(&pipeline, t0));
    assert_eq!(earliest(&pipeline, t0), None);
    dispatch_at(&mut pipeline, t0);
    assert!(lines.take().is_empty());

    dispatch_at(&mut pipeline, t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40032"), "{}", heard[0]);
    assert_eq!(
        field(&heard[0], "explained_wait_ms"),
        Some("0"),
        "{}",
        heard[0]
    );

    let kicked_at = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, kicked_at);
    assert_eq!(kicks(&pipeline), 1);
    dispatch_at(
        &mut pipeline,
        kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
    );
    assert_eq!(releases(&pipeline), 1);
    assert_eq!(queued(&pipeline, job_id), 0);
    assert_eq!(not_found(&pipeline) - not_found_before, 3);
}

// A batch seated on a lane that never comes back answers only within its
// window: once that has passed with no batch seated since, the stall it
// leaves is reported a whole interval after the last pass the batch
// explained, and kicked as any other.
#[tokio::test]
async fn a_seated_batch_that_never_answers_stops_explaining_the_stall() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40033);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());

    let t0 = Instant::now();
    liveness::seat_probe_round(&mut pipeline, job_id, 1, t0);
    let window_ends = t0 + liveness::SEATED_BATCH_ANSWERS_WITHIN;
    let step = DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    let mut at = t0;
    while at < window_ends {
        assert!(liveness::probe_answering(&pipeline, at));
        dispatch_at(&mut pipeline, at);
        at += step;
    }
    assert!(lines.take().is_empty(), "the batch explains the stall");
    assert_eq!(kicks(&pipeline), 0);

    assert!(!liveness::probe_answering(&pipeline, window_ends));
    assert_eq!(earliest(&pipeline, window_ends), None);
    let after = window_ends.max(t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER);
    dispatch_at(&mut pipeline, after);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40033"), "{}", heard[0]);
    assert_eq!(kicks(&pipeline), 1);
}
