// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The download dispatch stall watchdog: a stall nothing explains is kicked,
// then released. Every pass runs at an instant the test chooses, so the
// windows are crossed without waiting for them.

use super::*;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::pipeline::download::{
    DOWNLOAD_DISPATCH_STALL_KICK_AFTER, DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL,
    DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
};
use crate::pipeline::graph::disk::VolumeLeases;

// A pipeline with no connections: every pass with queued work ends with
// none open, which is the stall's shape. Its pool still has one server to
// rank, so the stall is not the outage the watchdog leaves alone.
async fn stalled_pipeline(temp_dir: &tempfile::TempDir) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline_with_backlog(temp_dir, 6 * 1024 * 1024, 0).await;
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port: refusing_harness_server_port(),
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    // Cooldowns that outlast the test, so a server taken out stays out
    // until the test brings it back.
    *pipeline.nntp.pool().health().lock().await = weaver_nntp::HealthTracker::new(
        1,
        weaver_nntp::HealthConfig {
            transient_cooldown: Duration::from_secs(60 * 60),
            ..weaver_nntp::HealthConfig::default()
        },
    );
    assert_eq!(
        pipeline
            .nntp
            .blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
            .map(|order| order.len()),
        Some(1)
    );
    pipeline
}

async fn insert_stalled_job(pipeline: &mut Pipeline, job_id: JobId, segments: usize) {
    insert_active_job(
        pipeline,
        job_id,
        segmented_job_spec(
            &format!("Stalled {}", job_id.0),
            "payload.bin",
            &vec![512; segments],
        ),
    )
    .await;
    let state = &pipeline.jobs[&job_id];
    assert_eq!(state.status, JobStatus::Downloading);
    assert_eq!(
        state.articles.active().unwrap().download_queue.len(),
        segments
    );
}

// Parks one of the job's queued articles as an infrastructure retry with no
// deadline, the way an unavailable provider parks it.
fn park_indefinite_infrastructure_retry(pipeline: &mut Pipeline, job_id: JobId) {
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.segment_number == 0)
        .pop()
        .expect("segment 0 is queued");
    let work = pipeline.dispatched(work);
    pipeline.note_retry_scheduled(work.segment_id);
    let scheduled_pool_generation = pipeline.pool_generation;
    pipeline.schedule_infrastructure_retry(
        None,
        crate::pipeline::RetryWork {
            scheduled_pool_generation,
            infrastructure_retry: true,
            hold: crate::pipeline::RetryHold::Ordinary,
            work,
        },
    );
}

fn kicks(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_dispatch_stall_kicks
        .load(Ordering::Relaxed)
}

fn releases(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_dispatch_stall_releases
        .load(Ordering::Relaxed)
}

#[tokio::test]
async fn a_stall_explained_by_a_future_deadline_is_never_kicked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    assert!(pipeline.download_stall_recovery);
    let propagating = JobId(40001);
    let durable_lead = JobId(40002);
    insert_stalled_job(&mut pipeline, propagating, 2).await;
    insert_stalled_job(&mut pipeline, durable_lead, 2).await;
    let t0 = Instant::now();
    let propagation_ends = t0 + Duration::from_secs(60 * 60);
    let durable_lead_ends = t0 + Duration::from_secs(30 * 60);
    pipeline
        .propagation_ready_at
        .insert(propagating, (propagation_ends, 0));
    pipeline
        .download_restart_durable_lead_retry_after
        .insert(durable_lead, durable_lead_ends);

    assert_eq!(
        pipeline.earliest_explained_dispatch_wait(t0),
        Some(durable_lead_ends)
    );
    for minutes in [0, 3, 13, 29] {
        pipeline.dispatch_downloads_at(t0 + Duration::from_secs(minutes * 60));
    }

    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    let watch = pipeline
        .download_dispatch_stall
        .as_ref()
        .expect("the stall is being watched");
    assert_eq!(watch.since, durable_lead_ends);
    assert!(watch.kick.is_none());
    assert_eq!(
        pipeline.jobs[&propagating]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(
        pipeline.jobs[&durable_lead]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert!(pipeline.propagation_ready_at.contains_key(&propagating));
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&durable_lead)
    );
}

// A job whose articles are being read waits on that read, which has no
// deadline: however long it takes, the stall it leaves is never kicked.
#[tokio::test]
async fn a_stall_behind_a_job_whose_articles_are_being_read_is_never_kicked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40010);
    super::article_scheduler::add_queued_job(&mut pipeline, job_id, 3).await;
    pipeline.db.set_nzb_admitted(job_id, true).unwrap();
    pipeline.nzb_admitted.insert(job_id);
    // The scheduler's first reach sends for the articles; the read is never
    // delivered here.
    pipeline.request_job_activation(job_id);
    let t0 = Instant::now();
    pipeline.dispatch_downloads_at(t0);
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        crate::jobs::model::JobArticles::Activating(..)
    ));
    assert_eq!(pipeline.download_scheduler_eligible_jobs(), vec![job_id]);

    let kick_due = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    assert_eq!(
        pipeline.earliest_explained_dispatch_wait(kick_due),
        Some(kick_due)
    );
    for at in [
        kick_due,
        kick_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
        kick_due + 2 * DOWNLOAD_DISPATCH_STALL_KICK_AFTER,
    ] {
        pipeline.dispatch_downloads_at(at);
    }

    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert!(matches!(
        pipeline.jobs[&job_id].articles,
        crate::jobs::model::JobArticles::Activating(..)
    ));
}

#[tokio::test]
async fn a_job_held_by_a_volume_at_its_write_budget_is_never_kicked_or_released() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40011);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .expect("the job writes through its volume's writer")
            .working()
            .volume(),
    );
    // The limit the volume's leases stop at, from the hard limit it was
    // given as the job was admitted.
    let budget = volume.lease_limit();
    assert!(budget < u64::MAX);
    let t0 = Instant::now();
    assert_eq!(pipeline.earliest_explained_dispatch_wait(t0), None);

    // The volume counts the budget: the job waits on the disk.
    volume.count_for_test(budget);
    assert_eq!(pipeline.earliest_explained_dispatch_wait(t0), Some(t0));
    let kick_due = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    let release_due = kick_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER;
    let long_after = release_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER;
    for at in [t0, kick_due, release_due, long_after] {
        pipeline.dispatch_downloads_at(at);
        assert_eq!(pipeline.earliest_explained_dispatch_wait(at), Some(at));
    }
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );

    // Below the budget the disk explains nothing any more.
    volume.count_for_test(budget - 1);
    assert_eq!(pipeline.earliest_explained_dispatch_wait(long_after), None);
}

#[tokio::test]
async fn an_unexplained_stall_is_kicked_once_at_the_kick_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40003);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    park_indefinite_infrastructure_retry(&mut pipeline, job_id);
    // One queued article rotated off the server, another excluded from a
    // server that answered 430 for it.
    {
        let queue = &mut pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue;
        let mut works = queue.drain_all();
        works[0].avoid_server = Some(0);
        works[1].exclude_servers = weaver_nntp::ServerSet::from([1]);
        for work in works {
            queue.push(work);
        }
    }
    let t0 = Instant::now();
    // Holds whose deadlines have passed explain nothing, but the scheduler
    // still keeps them until something clears them.
    pipeline
        .download_restart_durable_lead_retry_after
        .insert(job_id, t0);
    pipeline.propagation_ready_at.insert(job_id, (t0, 0));
    assert_eq!(pipeline.earliest_explained_dispatch_wait(t0), None);

    pipeline.dispatch_downloads_at(t0);
    let before_kick = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1);
    pipeline.dispatch_downloads_at(before_kick);
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(
        pipeline.last_download_dispatch_stall_log_at,
        Some(before_kick)
    );

    let kicked_at = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    pipeline.dispatch_downloads_at(kicked_at);

    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert!(
        !pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&job_id)
    );
    assert!(!pipeline.propagation_ready_at.contains_key(&job_id));
    // The hint is gone; the 430 exclusion is a fact and stays.
    let queue = &pipeline.jobs[&job_id]
        .articles
        .active()
        .unwrap()
        .download_queue;
    assert!(
        queue
            .peek_first_matching(|work| work.avoid_server.is_some())
            .is_none()
    );
    assert!(
        queue
            .peek_first_matching(|work| work.exclude_servers == vec![1])
            .is_some()
    );
    assert_eq!(queue.excluded_work_count(), 1);
    // The stall line is throttled to one per interval and the last one went
    // out a second ago: only the pass the kick ran could have written this.
    assert_eq!(
        pipeline.last_download_dispatch_stall_log_at,
        Some(kicked_at)
    );
    let watch = pipeline
        .download_dispatch_stall
        .as_ref()
        .expect("the stall is still being watched");
    assert_eq!(watch.kick.as_ref().map(|(at, _)| *at), Some(kicked_at));

    pipeline.dispatch_downloads_at(kicked_at + Duration::from_secs(60));
    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
}

#[tokio::test]
async fn a_kicked_stall_whose_bytes_never_move_is_released() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let wedged = JobId(40004);
    let moving = JobId(40005);
    insert_stalled_job(&mut pipeline, wedged, 100).await;
    insert_stalled_job(&mut pipeline, moving, 2).await;
    // Most of the wedged job's articles are already out of the queue, so the
    // three left missing leave it healthy enough for its completion check to
    // decide what it becomes, rather than its health failing it on the spot.
    let fetched = pipeline
        .jobs
        .get_mut(&wedged)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.segment_number >= 3);
    assert_eq!(fetched.len(), 97);
    let not_found_before = pipeline.metrics.articles_not_found.load(Ordering::Relaxed);
    let t0 = Instant::now();

    pipeline.dispatch_downloads_at(t0);
    let kicked_at = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    pipeline.dispatch_downloads_at(kicked_at);
    assert_eq!(kicks(&pipeline), 1);
    // One job's bytes move after the kick; the other's never do.
    pipeline.jobs.get_mut(&moving).unwrap().downloaded_bytes += 512;

    pipeline.dispatch_downloads_at(
        kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER - Duration::from_secs(1),
    );
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.jobs[&wedged]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );

    pipeline.dispatch_downloads_at(kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER);

    assert_eq!(releases(&pipeline), 1);
    assert_eq!(kicks(&pipeline), 1);
    assert!(
        pipeline.jobs[&wedged]
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed) - not_found_before,
        3
    );
    assert_eq!(pipeline.jobs[&wedged].failed_bytes, 3 * 512);
    assert!(pipeline.pending_completion_checks.contains(&wedged));
    assert_eq!(
        pipeline.jobs[&moving]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(pipeline.jobs[&moving].failed_bytes, 0);
    assert!(!pipeline.pending_completion_checks.contains(&moving));
    assert!(pipeline.download_dispatch_stall.is_none());
}

#[tokio::test]
async fn a_stall_with_no_server_to_dispatch_to_waits_for_one() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40007);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    park_indefinite_infrastructure_retry(&mut pipeline, job_id);
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_cooldown(0, weaver_nntp::CooldownReason::Transport);
    assert_eq!(
        pipeline
            .nntp
            .blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
            .map(|order| order.len()),
        Some(0)
    );
    let t0 = Instant::now();

    pipeline.dispatch_downloads_at(t0);
    let past_kick = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    pipeline.dispatch_downloads_at(past_kick);
    let past_release = past_kick + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER + Duration::from_secs(60);
    pipeline.dispatch_downloads_at(past_release);

    assert_eq!(
        pipeline.last_download_dispatch_stall_log_at,
        Some(past_release)
    );
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
    assert!(!pipeline.pending_completion_checks.contains(&job_id));

    // The server comes back right after the last pass that found it out:
    // the kick window opens from there, not from the stall's start.
    pipeline.nntp.pool().health().lock().await.record_success(0);
    let back_at = past_release;
    pipeline.dispatch_downloads_at(back_at + Duration::from_secs(60));
    assert_eq!(kicks(&pipeline), 0);
    pipeline.dispatch_downloads_at(
        back_at + DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
    );
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(pipeline.infrastructure_retries.len(), 1);

    pipeline.dispatch_downloads_at(back_at + DOWNLOAD_DISPATCH_STALL_KICK_AFTER);
    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
}

#[tokio::test]
async fn with_recovery_off_a_stall_is_only_logged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    pipeline.download_stall_recovery = false;
    let job_id = JobId(40006);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    park_indefinite_infrastructure_retry(&mut pipeline, job_id);
    let t0 = Instant::now();
    pipeline
        .download_restart_durable_lead_retry_after
        .insert(job_id, t0);

    pipeline.dispatch_downloads_at(t0);
    // One pass that opens nothing is not yet a stall.
    assert_eq!(pipeline.last_download_dispatch_stall_log_at, None);
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));
    let logged_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    pipeline.dispatch_downloads_at(logged_at);
    assert_eq!(
        pipeline.last_download_dispatch_stall_log_at,
        Some(logged_at)
    );
    let kick_window = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    pipeline.dispatch_downloads_at(kick_window);
    let past_release =
        kick_window + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER + Duration::from_secs(60);
    pipeline.dispatch_downloads_at(past_release);

    assert_eq!(
        pipeline.last_download_dispatch_stall_log_at,
        Some(past_release)
    );
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert!(pipeline.download_dispatch_stall.is_none());
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    assert_eq!(
        pipeline.jobs[&job_id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
    assert!(
        pipeline
            .download_restart_durable_lead_retry_after
            .contains_key(&job_id)
    );
    assert!(!pipeline.pending_completion_checks.contains(&job_id));
}

#[test]
fn stall_recovery_is_on_unless_the_variable_says_0_false_or_off() {
    use crate::pipeline::download::download_stall_recovery_from_env_value as parse;

    // On by default: production sets nothing.
    assert!(parse(None));
    assert!(parse(Some(String::new())));
    for enabled in ["1", "true", "on", "yes", "anything"] {
        assert!(parse(Some(enabled.to_string())), "{enabled}");
    }
    for disabled in ["0", "false", "off", "OFF", "False", " off "] {
        assert!(!parse(Some(disabled.to_string())), "{disabled}");
    }
}

// Regression fixtures use real dispatch gates and nonzero capacity. Aging
// the watch tests the release decision, not elapsed provider retry history.
async fn verification_pipeline(temp: &tempfile::TempDir, two_servers: bool) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline_with_backlog(temp, 6 * 1024 * 1024, 1).await;
    let mut config = weaver_nntp::client::NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port: refusing_harness_server_port(),
            tls: false,
            ..Default::default()
        },
        1,
    );
    if two_servers {
        config.servers.push(weaver_nntp::ServerPoolConfig::new(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port: refusing_harness_server_port(),
                tls: false,
                ..Default::default()
            },
        ));
    }
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(config));
    pipeline
}

fn verification_run_real_kick(pipeline: &mut Pipeline) {
    let now = Instant::now();
    pipeline.download_dispatch_stall = Some(crate::pipeline::download::DispatchStallWatch {
        since: now - DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
        segments_downloaded: pipeline.metrics.segments_downloaded.load(Ordering::Relaxed),
        kick: None,
    });
    pipeline.dispatch_downloads();
    assert_eq!(kicks(pipeline), 1);
    assert_eq!(releases(pipeline), 0);
    assert!(
        pipeline
            .download_dispatch_stall
            .as_ref()
            .unwrap()
            .kick
            .is_some()
    );
}

fn verification_old_kick(pipeline: &mut Pipeline, jobs: &[JobId]) {
    let now = Instant::now();
    pipeline.download_dispatch_stall = Some(crate::pipeline::download::DispatchStallWatch {
        since: now - DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER - Duration::from_secs(1),
        segments_downloaded: pipeline.metrics.segments_downloaded.load(Ordering::Relaxed),
        kick: Some((
            now - DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER - Duration::from_secs(1),
            jobs.iter()
                .map(|id| (*id, pipeline.jobs[id].downloaded_bytes))
                .collect(),
        )),
    });
}

async fn verification_exclude_healthy_and_cool_remaining(pipeline: &mut Pipeline, job_id: JobId) {
    let queue = &mut pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
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
    // A genuine auth rejection uses the unmodified production five-minute
    // backoff. The watchdog can have an older history than this latest
    // rejection. The server has refused the login for its whole give-up
    // time, so its refusal is the verdict, not a wait that holds the job:
    // a refusal still inside the give-up time is
    // `a_fresh_login_refusal_holds_the_stall_watchdog_until_its_give_up_time`.
    refuse_login_past_give_up(pipeline, 1).await;
    assert_eq!(
        pipeline
            .nntp
            .blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
            .unwrap()
            .iter()
            .map(|id| id.0)
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert!(
        pipeline
            .nntp
            .blocking_body_server_order(weaver_nntp::ServerSet::from([0]))
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn mixed_stall_kick_preserves_another_jobs_propagation_hold() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let wedged = JobId(41902);
    let waiting = JobId(41903);
    insert_stalled_job(&mut pipeline, wedged, 3).await;
    let mut spec = segmented_job_spec("Propagating", "payload.bin", &[512, 512, 512]);
    let posted = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    for file in &mut spec.files {
        file.posted_at_epoch = Some(posted);
    }
    insert_active_job(&mut pipeline, waiting, spec).await;
    pipeline.propagation_delay = Duration::from_secs(3600);
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, wedged).await;
    let hold = pipeline.propagation_hold_until(waiting).unwrap();
    verification_run_real_kick(&mut pipeline);
    assert_eq!(pipeline.propagation_ready_at[&waiting].0, hold);
    let kicked = &pipeline
        .download_dispatch_stall
        .as_ref()
        .unwrap()
        .kick
        .as_ref()
        .unwrap()
        .1;
    assert!(kicked.contains_key(&wedged));
    assert!(!kicked.contains_key(&waiting));
    assert_eq!(
        pipeline.jobs[&waiting]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn mixed_stall_release_rechecks_each_jobs_current_wait() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let wedged = JobId(41912);
    let waiting = JobId(41913);
    insert_stalled_job(&mut pipeline, wedged, 3).await;
    insert_stalled_job(&mut pipeline, waiting, 3).await;
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, wedged).await;
    // Both jobs belonged to the earlier kick; one now has a valid wait.
    verification_old_kick(&mut pipeline, &[wedged, waiting]);
    let hold = Instant::now() + Duration::from_secs(3600);
    pipeline.propagation_delay = Duration::from_secs(3600);
    pipeline.propagation_ready_at.insert(waiting, (hold, 0));
    pipeline.dispatch_downloads();
    assert_eq!(releases(&pipeline), 1);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        3
    );
    assert_eq!(
        pipeline.jobs[&waiting]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(pipeline.jobs[&waiting].failed_bytes, 0);
    assert_eq!(pipeline.propagation_ready_at[&waiting].0, hold);
}

#[tokio::test]
async fn operator_pause_and_resume_discard_pre_pause_stall_history() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41914);
    insert_stalled_job(&mut pipeline, job, 3).await;
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, job).await;
    verification_old_kick(&mut pipeline, &[job]);
    crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_all()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.download_dispatch_stall.is_none());
    pipeline.dispatch_downloads();
    assert!(pipeline.download_dispatch_stall.is_none());
    crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .resume_all()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.dispatch_downloads();
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(kicks(&pipeline), 0);
    assert!(
        pipeline
            .download_dispatch_stall
            .as_ref()
            .unwrap()
            .kick
            .is_none()
    );
    assert_eq!(
        pipeline.jobs[&job]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
}

#[tokio::test]
async fn a_valid_wait_after_kick_restarts_the_recovery_window() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41915);
    insert_stalled_job(&mut pipeline, job, 3).await;
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, job).await;
    verification_old_kick(&mut pipeline, &[job]);
    pipeline.propagation_delay = Duration::from_secs(3600);
    let ready_at = Instant::now() + Duration::from_secs(3600);
    pipeline.propagation_ready_at.insert(job, (ready_at, 0));
    pipeline.dispatch_downloads();
    let watch = pipeline.download_dispatch_stall.as_ref().unwrap();
    assert!(watch.kick.is_none());
    assert_eq!(watch.since, ready_at);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.jobs[&job]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
}

// Two fill servers: server 0 answered that it lacks the job's articles, and
// server 1 has just begun refusing the login. The articles wait for server 1
// as through an outage: while they wait the watchdog's clock restarts at
// each pass, its windows pass and nothing is booked, and the job names the
// server. Once server 1 has refused for its whole give-up time, the refusal
// is the verdict and the release books them as before.
#[tokio::test]
async fn a_fresh_login_refusal_holds_the_stall_watchdog_until_its_give_up_time() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41916);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
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
    refuse_login(&pipeline, 1).await;
    let queued = |pipeline: &Pipeline| {
        pipeline.jobs[&job]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
    };

    // The kick window has passed: the clock restarts now, not at the
    // verdict instant an hour ahead.
    let now = Instant::now();
    pipeline.download_dispatch_stall = Some(crate::pipeline::download::DispatchStallWatch {
        since: now - DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
        segments_downloaded: pipeline.metrics.segments_downloaded.load(Ordering::Relaxed),
        kick: None,
    });
    pipeline.dispatch_downloads();
    assert_eq!(kicks(&pipeline), 0);
    let watch = pipeline.download_dispatch_stall.as_ref().unwrap();
    assert!(watch.kick.is_none());
    assert!(watch.since >= now, "the clock restarts");
    assert!(
        watch.since <= Instant::now(),
        "at the pass, not at the verdict instant ahead"
    );
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );

    // A kick of long ago and its release window passed: nothing is booked.
    verification_old_kick(&mut pipeline, &[job]);
    pipeline.dispatch_downloads();
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert_eq!(queued(&pipeline), 3);
    assert_eq!(pipeline.jobs[&job].status, JobStatus::Downloading);
    assert_eq!(pipeline.jobs[&job].failed_bytes, 0);

    // Server 1 has refused the login for its whole give-up time.
    pipeline.nntp.pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    assert!(pipeline.nntp.pool().refuses_credentials(1));
    verification_old_kick(&mut pipeline, &[job]);
    pipeline.dispatch_downloads();
    assert_eq!(releases(&pipeline), 1);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        3
    );
}

// The wait reason naming pool servers `servers` as refusing the login.
fn login_refused_name(pipeline: &Pipeline, servers: &[usize]) -> String {
    let configs = pipeline.nntp.pool().server_configs();
    format!(
        "{}:{}",
        crate::jobs::handle::LOGIN_REFUSED_WAIT_REASON,
        servers
            .iter()
            .map(|idx| format!("{}:{}", configs[*idx].host, configs[*idx].port))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

// Server 1 has just begun refusing the login, but the job's download work
// all excludes it, and only its parked optional recovery (which dispatches
// nothing) could go there. The job does not wait on server 1: it shows no
// login wait and the watchdog kicks its stall as before.
#[tokio::test]
async fn a_login_refusal_holds_nothing_when_the_download_work_all_excludes_the_server() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41917);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let active = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap();
    let mut works = active.download_queue.drain_all();
    let parked = works.pop().unwrap();
    for work in &mut works {
        work.exclude_servers = weaver_nntp::ServerSet::from([0, 1]);
    }
    for work in works {
        active.download_queue.push(work);
    }
    active.recovery_queue.push(parked);
    refuse_login(&pipeline, 1).await;
    assert_eq!(
        pipeline.login_refusal_view().waiting,
        weaver_nntp::ServerSet::from([1])
    );
    assert_eq!(
        pipeline.job_login_refusal_wait(job, pipeline.login_refusal_view(), None),
        None
    );
    assert_eq!(
        pipeline.dispatch_wait_reason(job, pipeline.login_refusal_view()),
        None
    );
    verification_run_real_kick(&mut pipeline);
}

// Server 1 has just begun refusing the login, and server 0 may serve the
// job's work: nothing waits on server 1, the job shows no login wait and
// the watchdog is not held for it.
#[tokio::test]
async fn a_login_refusal_holds_nothing_while_another_server_may_serve_the_work() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41918);
    insert_stalled_job(&mut pipeline, job, 3).await;
    refuse_login(&pipeline, 1).await;
    let view = pipeline.login_refusal_view();
    assert_eq!(view.waiting, weaver_nntp::ServerSet::from([1]));
    assert_eq!(pipeline.job_login_refusal_wait(job, view, None), None);
    assert_eq!(pipeline.dispatch_wait_reason(job, view), None);
    assert_eq!(
        pipeline.earliest_explained_dispatch_wait(Instant::now()),
        None,
        "the stall is unexplained"
    );
}

// Server 1 refuses the login, then fails on transport inside the same run
// of refusals: its work still waits for it to the run's verdict instant.
// The watchdog's windows pass during the transport quarantine and nothing
// is booked; the job still names the server as refusing the login.
#[tokio::test]
async fn a_transport_quarantine_inside_a_login_refusal_run_keeps_the_work_waiting() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41919);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
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
    refuse_login(&pipeline, 1).await;
    pipeline.nntp.pool().quarantine_for_transport_for_test(1);
    assert!(!pipeline.nntp.pool().refuses_credentials(1));
    assert_eq!(
        pipeline.login_refusal_view().waiting,
        weaver_nntp::ServerSet::from([1])
    );

    let now = Instant::now();
    pipeline.download_dispatch_stall = Some(crate::pipeline::download::DispatchStallWatch {
        since: now - DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
        segments_downloaded: pipeline.metrics.segments_downloaded.load(Ordering::Relaxed),
        kick: None,
    });
    pipeline.dispatch_downloads();
    assert_eq!(kicks(&pipeline), 0);
    verification_old_kick(&mut pipeline, &[job]);
    pipeline.dispatch_downloads();
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.jobs[&job]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(pipeline.jobs[&job].failed_bytes, 0);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );
}

// The wait reason the job's queue row stores.
fn stored_reason(pipeline: &Pipeline, job: JobId) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(job)
            .and_then(|row| row.report.job.download_wait_reason.clone())
    })
}

// A stalled job whose three articles server 0 lacks, on the two-server
// pipeline, with server 1 just begun refusing the login and the row told.
async fn job_waiting_on_server_1(temp: &tempfile::TempDir, job: JobId) -> Pipeline {
    let mut pipeline = verification_pipeline(temp, true).await;
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
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
    refuse_login(&pipeline, 1).await;
    pipeline.report_changed_dispatch_holds();
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[1]))
    );
    pipeline
}

// The row stops saying the job waits on the login once the server takes it.
#[tokio::test]
async fn the_stored_login_wait_clears_once_the_login_is_taken() {
    let temp = tempfile::tempdir().unwrap();
    let job = JobId(41920);
    let mut pipeline = job_waiting_on_server_1(&temp, job).await;
    pipeline
        .nntp
        .pool()
        .take_recovery_probe_for_test(1)
        .answer();
    pipeline.report_changed_dispatch_holds();
    assert_eq!(stored_reason(&pipeline, job), None);
}

// The hour passes with no probe out and nothing else happening. The loop's
// wake for that instant tells the verdict once and asks for a dispatch
// pass, whose report restates the row: no later, unrelated pass is needed.
#[tokio::test]
async fn the_stored_login_wait_clears_at_the_verdict_instant_with_no_probe_out() {
    let temp = tempfile::tempdir().unwrap();
    let job = JobId(41921);
    let mut pipeline = job_waiting_on_server_1(&temp, job).await;
    assert!(
        pipeline.nntp.pool().next_login_refusal_verdict().is_some(),
        "the loop has an instant to wait for"
    );
    pipeline.tell_login_refusal_verdicts();
    assert!(!pipeline.download_dispatch_wake, "the hour is not up");
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[1]))
    );

    pipeline.nntp.pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    pipeline.tell_login_refusal_verdicts();
    assert!(pipeline.download_dispatch_wake);
    assert_eq!(
        pipeline.nntp.pool().next_login_refusal_verdict(),
        None,
        "told once: nothing is left to wake for"
    );
    assert!(
        pipeline
            .nntp
            .pool()
            .login_refusal_verdicts_untold()
            .is_none()
    );
    pipeline.report_changed_dispatch_holds();
    assert_eq!(stored_reason(&pipeline, job), None);
}

// The refusing server is removed from the pool: the row stops naming it.
#[tokio::test]
async fn the_stored_login_wait_clears_once_the_server_is_removed() {
    let temp = tempfile::tempdir().unwrap();
    let job = JobId(41922);
    let mut pipeline = job_waiting_on_server_1(&temp, job).await;
    let kept = pipeline.nntp.pool().server_configs()[0].clone();
    let generation = pipeline.pool_generation + 1;
    pipeline.activate_nntp(
        weaver_nntp::NntpClient::new(weaver_nntp::client::NntpClientConfig::single(kept, 1)),
        1,
        generation,
    );
    assert!(pipeline.login_refusal_view().waiting.is_empty());
    pipeline.report_changed_dispatch_holds();
    assert_eq!(stored_reason(&pipeline, job), None);
}

// A server's password is edited while it refuses the login and a job waits
// on it. A pool is built from the new password while the pool of the old
// one is still in use: a dispatch pass in between is not handed the probe
// and the run of refusals goes on, as it would if the rebuild failed there.
// Once the new pool is put in use its probe is due at once, the run is
// over and the row stops saying the job waits on a refused login.
#[tokio::test]
async fn a_credential_edit_ends_the_login_wait_and_makes_the_new_pools_probe_due() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_backlog(&temp, 6 * 1024 * 1024, 1).await;
    let registry = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = registry.control(weaver_nntp::transfer::StableServerId(9));
    let port = refusing_harness_server_port();
    let client = |password: &str| {
        let mut config = weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port,
                tls: false,
                username: Some("user".into()),
                password: Some(password.into()),
                ..Default::default()
            },
            1,
        );
        config.servers[0].stable_id = weaver_nntp::transfer::StableServerId(9);
        config.servers[0].transfer_control = Some(Arc::clone(&control));
        weaver_nntp::NntpClient::new(config)
    };
    pipeline.nntp = Arc::new(client("old"));
    let job = JobId(41923);
    insert_stalled_job(&mut pipeline, job, 3).await;
    refuse_login(&pipeline, 0).await;
    pipeline.report_changed_dispatch_holds();
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[0]))
    );

    // The new pool exists; the old one is still the pool in use.
    let verdict_at = control.login_refusal_verdict_at();
    let rebuilt = client("new");
    pipeline.dispatch_downloads();
    assert!(
        matches!(
            pipeline.nntp.pool().connection_hold(0, true),
            Some(weaver_nntp::recovery::ConnectionHold::Until(_))
        ),
        "the pool of the old password is not handed the probe"
    );
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    pipeline.report_changed_dispatch_holds();
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[0])),
        "a rebuild that failed here would leave the wait as it was"
    );
    let generation = pipeline.pool_generation + 1;
    pipeline.activate_nntp(rebuilt, 1, generation);
    assert_eq!(
        pipeline.nntp.pool().connection_hold(0, true),
        Some(weaver_nntp::recovery::ConnectionHold::Probe)
    );
    assert_eq!(control.login_refusal_verdict_at(), None);
    assert!(pipeline.login_refusal_view().waiting.is_empty());
    pipeline.report_changed_dispatch_holds();
    assert_eq!(stored_reason(&pipeline, job), None);
}

// Two of the job's articles wait on server 1, which refuses the login, and
// the third is work server 0 may serve that nothing is fetching. The job
// names server 1, but the wedged third article is not explained by it: the
// watchdog is not held.
#[tokio::test]
async fn a_login_wait_on_some_of_a_jobs_work_does_not_explain_the_rest_being_wedged() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41924);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut works = queue.drain_all();
    for work in works.iter_mut().take(2) {
        work.exclude_servers = weaver_nntp::ServerSet::from([0]);
    }
    for work in works {
        queue.push(work);
    }
    refuse_login(&pipeline, 1).await;
    let view = pipeline.login_refusal_view();
    assert_eq!(
        pipeline.job_login_refusal_wait(job, view, None),
        Some(crate::pipeline::download::LoginRefusalWait {
            servers: weaver_nntp::ServerSet::from([1]),
            every: false,
        })
    );
    assert_eq!(
        pipeline.dispatch_wait_reason(job, view).as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );
    assert_eq!(
        pipeline.earliest_explained_dispatch_wait(Instant::now()),
        None,
        "the stall is unexplained"
    );
}

// Two jobs wait on the one server refusing the login, with no probe out for
// either. When the hour lands the verdict is on both: the turn that tells
// it books the waiting articles of each, not only those of a job a later
// probe happens to carry.
#[tokio::test]
async fn the_verdict_instant_books_every_job_that_waited_on_the_server() {
    let temp = tempfile::tempdir().unwrap();
    let first = JobId(41925);
    let second = JobId(41926);
    let mut pipeline = two_jobs_waiting_on_server_1(&temp, first, second).await;
    let queued = queued_for_download;
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(
        (queued(&pipeline, first), queued(&pipeline, second)),
        (3, 3)
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0,
        "inside the hour nothing is booked"
    );

    pipeline.nntp.pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(
        (queued(&pipeline, first), queued(&pipeline, second)),
        (0, 0)
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        6
    );
}

// Two jobs, each with all its work waiting on server 1, which refuses the
// login.
async fn two_jobs_waiting_on_server_1(
    temp: &tempfile::TempDir,
    first: JobId,
    second: JobId,
) -> Pipeline {
    let mut pipeline = job_waiting_on_server_1(temp, first).await;
    insert_stalled_job(&mut pipeline, second, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&second)
        .unwrap()
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
    pipeline
}

// How many articles `job` has queued for download.
fn queued_for_download(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline
        .jobs
        .get(&job)
        .and_then(|state| state.articles.active())
        .map_or(0, |active| active.download_queue.len())
}

// The hour lands when the quarantine's deadline already has: the probe is
// due, the server does not refuse just then, and the sweep has nothing to
// book. It must not mark the verdict told for that: when the probe is then
// refused, the verdict is still to be told and both jobs are booked.
#[tokio::test]
async fn a_verdict_instant_with_the_probe_due_tells_nothing_until_the_probe_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let first = JobId(41928);
    let second = JobId(41929);
    let mut pipeline = two_jobs_waiting_on_server_1(&temp, first, second).await;
    let pool = Arc::clone(pipeline.nntp.pool());
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    pool.set_login_refusal_clock(1, Instant::now() + give_up);
    pool.expire_quarantine_for_test(1);
    assert!(!pool.refuses_credentials(1), "the probe is due");
    pipeline.tell_login_refusal_verdicts();
    assert!(!pipeline.download_dispatch_wake);
    assert_eq!(
        (
            queued_for_download(&pipeline, first),
            queued_for_download(&pipeline, second)
        ),
        (3, 3)
    );
    assert_eq!(
        pool.next_login_refusal_verdict(),
        None,
        "nothing to wake for until the server refuses again"
    );

    pool.refuse_login_for_test(1, give_up);
    assert!(pool.refuses_credentials(1));
    assert!(
        pool.next_login_refusal_verdict()
            .is_some_and(|at| at <= Instant::now() + give_up),
        "the verdict is still to be told"
    );
    assert!(pool.login_refusal_verdicts_untold().is_some());
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(
        (
            queued_for_download(&pipeline, first),
            queued_for_download(&pipeline, second)
        ),
        (0, 0)
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        6
    );
    assert!(pool.login_refusal_verdicts_untold().is_none());
    assert_eq!(pool.next_login_refusal_verdict(), None);
}

// The loop itself, with nothing else to wake it: a job waits on a server
// refusing the login, no probe is out, and the run's give-up time comes up.
// The loop's own wait for that instant ends, the waiting articles are
// booked and the verdict is told. The loop's clock is the paused one, so
// the instant is reached only by the wait the loop set for it: the system
// clock the turn's own check reads has not moved.
#[tokio::test(start_paused = true)]
async fn the_loop_wakes_itself_at_the_verdict_instant_and_books_the_waiting_work() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41930);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
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
    let pool = Arc::clone(pipeline.nntp.pool());
    // The run gives up before the quarantine's deadline, so the wait for
    // the verdict is the first the loop has set on this server.
    pool.refuse_login_for_test(1, Duration::from_mins(4));
    let verdict_at = pool
        .next_login_refusal_verdict()
        .expect("the loop has an instant to wait for");
    assert!(verdict_at > Instant::now());
    // The server's run is read as its give-up time old from here on; only
    // the loop's wait has still to come up.
    pool.set_login_refusal_clock(1, verdict_at);
    assert!(pool.login_refusal_verdicts_untold().is_some());

    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let metrics = Arc::clone(&pipeline.metrics);
    let mut health = pool.server_health_changes();
    let runner = tokio::spawn(async move { pipeline.run().await });
    while pool.login_refusal_verdicts_untold().is_some() {
        health.changed().await;
    }
    assert_eq!(pool.next_login_refusal_verdict(), None, "told once");
    assert_eq!(metrics.articles_not_found.load(Ordering::Relaxed), 3);
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// Two of the job's articles wait on server 1, which refuses the login, and
// the third is one both servers were tried for and nothing has booked. The
// watchdog's release books that one and leaves the two that wait for the
// server's verdict in the queue.
#[tokio::test]
async fn the_stall_release_leaves_the_articles_waiting_on_a_refused_login() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41927);
    insert_stalled_job(&mut pipeline, job, 3).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut works = queue.drain_all();
    for (n, work) in works.iter_mut().enumerate() {
        work.exclude_servers = if n < 2 {
            weaver_nntp::ServerSet::from([0])
        } else {
            weaver_nntp::ServerSet::from([0, 1])
        };
    }
    for work in works {
        queue.push(work);
    }
    refuse_login(&pipeline, 1).await;
    let view = pipeline.login_refusal_view();
    assert_eq!(
        pipeline.earliest_explained_dispatch_wait(Instant::now()),
        None,
        "the job is not explained by the login wait, so a release takes it"
    );
    let released = pipeline.take_stall_released_work(job, view);
    assert_eq!(
        released
            .iter()
            .map(|work| work.exclude_servers)
            .collect::<Vec<_>>(),
        [weaver_nntp::ServerSet::from([0, 1])]
    );
    let left = &pipeline.jobs[&job]
        .articles
        .active()
        .unwrap()
        .download_queue;
    assert_eq!(left.len(), 2);
    assert_eq!(
        left.steered_exclusion_sets().collect::<Vec<_>>(),
        [weaver_nntp::ServerSet::from([0])],
        "the two that wait"
    );

    // With no server refusing the login, a release takes everything.
    pipeline
        .nntp
        .pool()
        .take_recovery_probe_for_test(1)
        .answer();
    let view = pipeline.login_refusal_view();
    assert_eq!(pipeline.take_stall_released_work(job, view).len(), 2);
}

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
    pipeline.download_stall_recovery = false;
    let verifying = JobId(40010);
    let downloading = JobId(40011);
    insert_stalled_job(&mut pipeline, verifying, 4).await;
    insert_stalled_job(&mut pipeline, downloading, 2).await;
    {
        let state = pipeline.jobs.get_mut(&verifying).unwrap();
        let active = state.articles.active_mut().unwrap();
        for work in active.download_queue.drain_all() {
            active.recovery_queue.push(work);
        }
        state.status = JobStatus::Verifying;
    }
    assert_eq!(
        pipeline.job_order.iter().position(|id| *id == verifying),
        Some(0),
        "the verifying job is the first the line could pick"
    );
    assert!(pipeline.nzb_admitted.contains(&verifying));
    assert!(pipeline.nzb_admitted.contains(&downloading));

    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    pipeline.dispatch_downloads_at(t0);
    assert!(
        lines.take().is_empty(),
        "the first pass only starts the clock"
    );
    pipeline.dispatch_downloads_at(t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    assert_eq!(pipeline.articles_in_flight, 0);
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
        let active = pipeline
            .jobs
            .get_mut(&verifying)
            .unwrap()
            .articles
            .active_mut()
            .unwrap();
        for work in active.recovery_queue.drain_all() {
            active.download_queue.push(work);
        }
    }
    let t1 = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    pipeline.dispatch_downloads_at(t1);
    assert_eq!(pipeline.articles_in_flight, 0);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40010"), "{}", heard[0]);
    assert_eq!(field(&heard[0], "parked_recovery_len"), Some("0"));
    assert_eq!(field(&heard[0], "download_queue_len"), Some("4"));
    assert_eq!(field(&heard[0], "dispatchable_queue_depth"), Some("6"));
    assert_eq!(pipeline.last_download_dispatch_stall_log_at, Some(t1));
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
    pipeline.download_stall_recovery = false;
    let job_id = JobId(40020);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();

    pipeline.dispatch_downloads_at(t0);
    let almost = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_millis(1);
    pipeline.dispatch_downloads_at(almost);
    assert_eq!(lines.take(), Vec::<String>::new());
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));

    let lasted = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    pipeline.dispatch_downloads_at(lasted);
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
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .expect("the job writes through its volume's writer")
            .working()
            .volume(),
    );
    let budget = volume.lease_limit();
    volume.count_for_test(budget);
    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();

    for at in [
        t0,
        t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL,
        t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER,
    ] {
        pipeline.dispatch_downloads_at(at);
    }
    assert_eq!(lines.take(), Vec::<String>::new());

    // Below the limit the same passes are a stall again, and warned of.
    volume.count_for_test(budget - 1);
    let later = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    pipeline.dispatch_downloads_at(later);
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
    pipeline.download_stall_recovery = false;
    let job_id = JobId(40012);
    insert_stalled_job(&mut pipeline, job_id, 2).await;

    let lines = LivenessLines::default();
    let _capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    pipeline.dispatch_downloads_at(t0);
    assert!(lines.take().is_empty(), "the first pass says nothing");
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));

    // A pass with an article in flight clears the clock. The article is
    // only counted, held by no lane, so the lane audit stays off for it.
    let busy_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_secs(1);
    pipeline.audit_lane_holdings = false;
    pipeline.articles_in_flight = 1;
    pipeline.dispatch_downloads_at(busy_at);
    pipeline.articles_in_flight = 0;
    pipeline.audit_lane_holdings = true;
    assert!(lines.take().is_empty());
    assert_eq!(pipeline.download_dispatch_liveness_since, None);

    // The interval since the first pass has passed, but not since the
    // condition came back.
    let back_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    pipeline.dispatch_downloads_at(back_at);
    assert!(lines.take().is_empty(), "the clock started over");
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(back_at));

    pipeline.dispatch_downloads_at(back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
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
    pipeline.download_stall_recovery = false;
    insert_stalled_job(&mut pipeline, job_id, 2).await;
    let lines = LivenessLines::default();
    let capture = tracing::subscriber::set_default(lines.clone());
    let t0 = Instant::now();
    pipeline.dispatch_downloads_at(t0);
    assert!(lines.take().is_empty());
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));
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
    assert_eq!(pipeline.download_dispatch_liveness_since, None);
    let back_at = t0 + Duration::from_secs(60 * 60);
    pipeline.dispatch_downloads_at(back_at);
    assert!(lines.take().is_empty(), "the clock started over");
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(back_at));

    let inside = back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL - Duration::from_secs(1);
    pipeline.dispatch_downloads_at(inside);
    assert!(lines.take().is_empty());

    pipeline.dispatch_downloads_at(back_at + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
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

    pipeline.nntp_handoff_draining = true;
    pipeline.dispatch_downloads_at(t0 + Duration::from_secs(60));
    pipeline.nntp_handoff_draining = false;
    assert_the_liveness_clock_started_over(&mut pipeline, &lines, t0, "40013");
}

// So does an operator pause.
#[tokio::test]
async fn an_operator_pause_starts_the_liveness_clock_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, JobId(40014)).await;

    let shared = pipeline.shared_state.clone();
    let all = crate::jobs::HoldOwner::All;
    let set_paused = |paused: bool| match paused {
        true => shared.hold(crate::jobs::Hold::manual(all)),
        false => shared.release(crate::jobs::HoldOrigin::Manual, all),
    };
    set_paused(true);
    pipeline.dispatch_downloads_at(t0 + Duration::from_secs(60));
    set_paused(false);
    assert_the_liveness_clock_started_over(&mut pipeline, &lines, t0, "40014");
}

// And so does a pass that finds nothing dispatch would serve.
#[tokio::test]
async fn a_pass_with_no_queued_work_starts_the_liveness_clock_over() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40015);
    let (mut pipeline, lines, _capture, t0) = liveness_clock_started(&temp_dir, job_id).await;

    fn active(pipeline: &mut Pipeline, job_id: JobId) -> &mut crate::jobs::model::ActiveArticles {
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
    }
    let parked: Vec<_> = active(&mut pipeline, job_id)
        .download_queue
        .drain_all()
        .into_iter()
        .collect();
    pipeline.dispatch_downloads_at(t0 + Duration::from_secs(60));
    for work in parked {
        active(&mut pipeline, job_id).download_queue.push(work);
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
    pipeline.rate_limiter.set_rate(Some(1));
    pipeline.rate_limiter.consume(u64::from(u32::MAX));
    assert!(pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads_at(t0 + Duration::from_secs(60));
    assert!(lines.take().is_empty());
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));

    pipeline.rate_limiter.set_rate(None);
    assert!(!pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads_at(t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
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

    pipeline.decode_backlog_budget_bytes = 1000;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(700, Ordering::Relaxed);
    let turned_away_at = t0 + Duration::from_secs(60);
    pipeline.download_pressure_soft_dispatch_after =
        Some(turned_away_at + Duration::from_secs(60 * 60));
    pipeline.dispatch_downloads_at(turned_away_at);
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        crate::DownloadPressureState::Soft.as_code()
    );
    assert_eq!(
        pipeline.download_pressure_soft_dispatch_after,
        Some(turned_away_at + Duration::from_secs(60 * 60)),
        "the pass was turned away"
    );
    assert!(lines.take().is_empty());
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));

    // Still under soft pressure, a pass that is let through writes the line.
    pipeline.download_pressure_soft_dispatch_after = None;
    pipeline.dispatch_downloads_at(t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        crate::DownloadPressureState::Soft.as_code()
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
    kind: DownloadFailureKind,
) -> (u64, Vec<String>) {
    use crate::pipeline::download::scheduler::Handout;

    insert_stalled_job(pipeline, job, 4).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
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
    let pool = Arc::clone(pipeline.nntp.pool());
    pool.quarantine_for_transport_for_test(1);
    let not_found_before = pipeline.metrics.articles_not_found.load(Ordering::Relaxed);

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
        let next_retry = pipeline.infrastructure_retries.next_deadline();
        let next = next_retry.map_or(next_probe, |retry| retry.min(next_probe));
        if next > end {
            break;
        }
        tokio::time::advance(next.saturating_duration_since(tokio::time::Instant::now())).await;
        let now = tokio::time::Instant::now();
        let mut event = Vec::new();
        let due = pipeline.infrastructure_retries.take_due(now);
        if !due.is_empty() {
            event.push(format!("{} retries due", due.len()));
        }
        for retry in due {
            pipeline.receive_retry_work(retry);
        }
        if now >= next_probe {
            next_probe += Duration::from_secs(60);
            pool.expire_quarantine_for_test(1);
            let pressure = pipeline.refresh_download_pressure();
            let works = match pipeline.next_works(1, 1, None, pressure) {
                Handout::Works(works) => works,
                _ => Vec::new(),
            };
            let probe = pool.take_recovery_probe_for_test(1);
            for work in works {
                event.push(format!("probe fails {}", work.segment_id.segment_number));
                let (segment_id, retry_count, exclude_servers) =
                    (work.segment_id, work.retry_count, work.exclude_servers);
                pipeline.hold_test_lane_works(vec![work], false);
                pipeline
                    .handle_download_done(DownloadResult {
                        lane_id: 0,
                        job_id: job,
                        runtime_generation: pipeline.pool_generation,
                        segment_id,
                        data: Err(DownloadError::fetch(kind, "probe failed")),
                        attempts: Vec::new(),
                        lane_observation: None,
                        source_server_idx: Some(1),
                        origin: DownloadResultOrigin::NormalPrimary,
                        retry_count,
                        exclude_servers,
                        release_connection_slot: true,
                    })
                    .await;
            }
            drop(probe);
            pool.quarantine_for_transport_for_test(1);
        }
        pipeline.dispatch_downloads_at(now.into_std());
        let parked: Vec<String> = pipeline
            .infrastructure_retries
            .iter_with_deadlines()
            .map(|(deadline, retry)| {
                format!(
                    "{}@{}",
                    retry.work.segment_id.segment_number,
                    deadline.map_or("none".into(), |at| rel(at.into_std()))
                )
            })
            .collect();
        let watch = pipeline.download_dispatch_stall.as_ref().map(|watch| {
            format!(
                "since {} kicked {:?}",
                rel(watch.since),
                watch.kick.as_ref().map(|(at, _)| rel(*at))
            )
        });
        trace.push(format!(
            "t={} [{}] watch={:?} explained={:?} parked={parked:?} servers={:?} kicks={} releases={}",
            rel(now.into_std()),
            event.join(", "),
            watch,
            pipeline
                .earliest_explained_dispatch_wait(now.into_std())
                .map(rel),
            pipeline
                .nntp
                .blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
                .map(|order| order.len()),
            kicks(pipeline),
            releases(pipeline),
        ));
        if releases(pipeline) > 0 {
            break;
        }
    }
    (
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed) - not_found_before,
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
        DownloadFailureKind::EstablishedTransport,
    )
    .await;
    let trace = trace.join("\n");
    assert_eq!(kicks(&pipeline), 1, "kicked once:\n{trace}");
    assert_eq!(releases(&pipeline), 1, "released:\n{trace}");
    assert!(booked > 0, "queued articles booked missing:\n{trace}");
    assert_eq!(
        pipeline.jobs[&job]
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
        DownloadFailureKind::ConnectionEstablishment,
    )
    .await;
    let trace = trace.join("\n");
    assert_eq!(kicks(&pipeline), 1, "kicked once:\n{trace}");
    assert_eq!(releases(&pipeline), 1, "released:\n{trace}");
    assert!(booked > 0, "queued articles booked missing:\n{trace}");
    assert_eq!(
        pipeline.jobs[&job]
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
        (crate::pipeline::RetryHold::Ordinary, true),
        (crate::pipeline::RetryHold::ProbeHold, false),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = stalled_pipeline(&temp_dir).await;
        let job_id = JobId(40020);
        insert_stalled_job(&mut pipeline, job_id, 3).await;
        let work = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .extract_matching(|work| work.segment_id.segment_number == 0)
            .pop()
            .expect("segment 0 is queued");
        let work = pipeline.dispatched(work);
        pipeline.note_retry_scheduled(work.segment_id);
        let scheduled_pool_generation = pipeline.pool_generation;
        pipeline.schedule_infrastructure_retry(
            Some(Duration::from_secs(90)),
            crate::pipeline::RetryWork {
                scheduled_pool_generation,
                infrastructure_retry: true,
                hold,
                work,
            },
        );
        let deadline = pipeline
            .infrastructure_retries
            .next_deadline()
            .expect("the retry is parked with a deadline")
            .into_std();
        let now = Instant::now();
        assert!(deadline > now);
        assert_eq!(
            pipeline.earliest_explained_dispatch_wait(now),
            explains.then_some(deadline),
            "{hold:?}"
        );
    }
}

fn queued(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline.jobs[&job_id]
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

fn not_found(pipeline: &Pipeline) -> u64 {
    pipeline.metrics.articles_not_found.load(Ordering::Relaxed)
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
    assert!(pipeline.download_stall_recovery);
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
        pipeline.post_probes.seat_round_for_test(probing, 0, 2, at);
        pipeline.dispatch_downloads_at(at);
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
    assert_eq!(pipeline.jobs[&probing].failed_bytes, 0);
    assert_eq!(pipeline.jobs[&beside].failed_bytes, 0);
    assert!(!pipeline.pending_completion_checks.contains(&probing));
    assert!(!pipeline.pending_completion_checks.contains(&beside));
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
    pipeline.post_probes.seat_round_for_test(job_id, 0, 0, t0);
    assert!(!pipeline.post_probes.answering(t0));
    assert_eq!(pipeline.earliest_explained_dispatch_wait(t0), None);
    pipeline.dispatch_downloads_at(t0);
    assert!(lines.take().is_empty());

    pipeline.dispatch_downloads_at(t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL);
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
    pipeline.dispatch_downloads_at(kicked_at);
    assert_eq!(kicks(&pipeline), 1);
    pipeline.dispatch_downloads_at(kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER);
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
    pipeline.post_probes.seat_round_for_test(job_id, 0, 1, t0);
    let window_ends = t0 + crate::pipeline::post_health::SEATED_BATCH_ANSWERS_WITHIN;
    let step = DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    let mut at = t0;
    while at < window_ends {
        assert!(pipeline.post_probes.answering(at));
        pipeline.dispatch_downloads_at(at);
        at += step;
    }
    assert!(lines.take().is_empty(), "the batch explains the stall");
    assert_eq!(kicks(&pipeline), 0);

    assert!(!pipeline.post_probes.answering(window_ends));
    assert_eq!(pipeline.earliest_explained_dispatch_wait(window_ends), None);
    let after = window_ends.max(t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER);
    pipeline.dispatch_downloads_at(after);
    let heard = lines.take();
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert_eq!(field(&heard[0], "job_id"), Some("40033"), "{}", heard[0]);
    assert_eq!(kicks(&pipeline), 1);
}
