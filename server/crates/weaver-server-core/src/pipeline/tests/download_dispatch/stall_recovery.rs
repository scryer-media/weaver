// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The download dispatch stall watchdog: a stall nothing explains is kicked,
// then released. Every pass runs at an instant the test chooses, so the
// windows are crossed without waiting for them.

use super::*;
use crate::pipeline::download::{
    DOWNLOAD_DISPATCH_STALL_KICK_AFTER, DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL,
    DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
};

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
    assert_eq!(pipeline.last_download_dispatch_stall_log_at, None);
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));
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
    // backoff. The watchdog can have an older history than this latest rejection.
    pipeline
        .nntp
        .pool()
        .health()
        .lock()
        .await
        .record_failure(1, true);
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

    let queue = pipeline.shared_state.queue.clone();
    let set_paused = |paused: bool| {
        queue.write(|inner| {
            inner.globals.paused = paused;
            crate::pipeline::queue::Change::changed(())
        })
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
    pipeline.rate_limiter.set_rate(1);
    pipeline.rate_limiter.consume(u64::from(u32::MAX));
    assert!(pipeline.rate_limiter.should_wait());
    pipeline.dispatch_downloads_at(t0 + Duration::from_secs(60));
    assert!(lines.take().is_empty());
    assert_eq!(pipeline.download_dispatch_liveness_since, Some(t0));

    pipeline.rate_limiter.set_rate(0);
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
