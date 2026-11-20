// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};
use weaver_server_composition::{JobId, JobStatus};
use weaver_server_pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::{
    control_fixture::completion_pending,
    decode_fixture::generation,
    dispatch_fixture::volume_budget::volume_lease_limit,
    insert_active_job,
    provider_fixture::dispatch,
    queue_fixture::{apply_intents, handle, metrics},
    refuse_login, refusing_harness_server_port, segmented_job_spec,
    stall_fixture::{client as client_of, job as stall_job, *},
};

#[tokio::test]
async fn a_stall_explained_by_a_future_deadline_is_never_kicked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    assert!(recovery(&pipeline));
    let propagating = JobId(40001);
    let durable_lead = JobId(40002);
    insert_stalled_job(&mut pipeline, propagating, 2).await;
    insert_stalled_job(&mut pipeline, durable_lead, 2).await;
    let t0 = Instant::now();
    let propagation_ends = t0 + Duration::from_secs(60 * 60);
    let durable_lead_ends = t0 + Duration::from_secs(30 * 60);
    propagation(&mut pipeline).insert(propagating, (propagation_ends, 0));
    durable(&mut pipeline).insert(durable_lead, durable_lead_ends);

    assert_eq!(earliest(&pipeline, t0), Some(durable_lead_ends));
    for minutes in [0, 3, 13, 29] {
        dispatch_at(&mut pipeline, t0 + Duration::from_secs(minutes * 60));
    }

    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    let since = stall_since(&pipeline).expect("the stall is being watched");
    assert_eq!(since, durable_lead_ends);
    assert!(kick(&pipeline).is_none());
    assert_eq!(
        stall_job(&pipeline, propagating)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(
        stall_job(&pipeline, durable_lead)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert!(propagation(&mut pipeline).contains_key(&propagating));
    assert!(durable(&mut pipeline).contains_key(&durable_lead));
}

// A job whose articles are being read waits on that read, which has no
// deadline: however long it takes, the stall it leaves is never kicked.
#[tokio::test]
async fn a_stall_behind_a_job_whose_articles_are_being_read_is_never_kicked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40010);
    activate_queued(&mut pipeline, job_id, 3).await;
    let t0 = Instant::now();
    dispatch_at(&mut pipeline, t0);
    assert!(matches!(
        stall_job(&pipeline, job_id).articles,
        weaver_server_pipeline::jobs::model::JobArticles::Activating(..)
    ));
    assert_eq!(eligible(&pipeline), vec![job_id]);

    let kick_due = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    assert_eq!(earliest(&pipeline, kick_due), Some(kick_due));
    for at in [
        kick_due,
        kick_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
        kick_due + 2 * DOWNLOAD_DISPATCH_STALL_KICK_AFTER,
    ] {
        dispatch_at(&mut pipeline, at);
    }

    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert!(matches!(
        stall_job(&pipeline, job_id).articles,
        weaver_server_pipeline::jobs::model::JobArticles::Activating(..)
    ));
}

#[tokio::test]
async fn a_job_held_by_a_volume_at_its_write_budget_is_never_kicked_or_released() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40011);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    let volume = working_volume(&pipeline, job_id);
    // The limit the volume's leases stop at, from the hard limit it was
    // given as the job was admitted.
    let budget = volume_lease_limit(&volume);
    assert!(budget < u64::MAX);
    let t0 = Instant::now();
    assert_eq!(earliest(&pipeline, t0), None);

    // The volume counts the budget: the job waits on the disk.
    volume.count_for_test(budget);
    assert_eq!(earliest(&pipeline, t0), Some(t0));
    let kick_due = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    let release_due = kick_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER;
    let long_after = release_due + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER;
    for at in [t0, kick_due, release_due, long_after] {
        dispatch_at(&mut pipeline, at);
        assert_eq!(earliest(&pipeline, at), Some(at));
    }
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );

    // Below the budget the disk explains nothing any more.
    volume.count_for_test(budget - 1);
    assert_eq!(earliest(&pipeline, long_after), None);
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
        let queue = &mut job_mut(&mut pipeline, job_id)
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
    durable(&mut pipeline).insert(job_id, t0);
    propagation(&mut pipeline).insert(job_id, (t0, 0));
    assert_eq!(earliest(&pipeline, t0), None);

    dispatch_at(&mut pipeline, t0);
    let before_kick = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1);
    dispatch_at(&mut pipeline, before_kick);
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(infra_count(&pipeline), 1);
    assert_eq!(
        stall_job(&pipeline, job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(last_log(&pipeline), Some(before_kick));

    let kicked_at = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, kicked_at);

    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(infra_count(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert!(!durable(&mut pipeline).contains_key(&job_id));
    assert!(!propagation(&mut pipeline).contains_key(&job_id));
    // The hint is gone; the 430 exclusion is a fact and stays.
    let queue = &stall_job(&pipeline, job_id)
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
    assert_eq!(last_log(&pipeline), Some(kicked_at));
    stall_since(&pipeline).expect("the stall is still being watched");
    assert_eq!(kick(&pipeline).map(|(at, _)| *at), Some(kicked_at));

    dispatch_at(&mut pipeline, kicked_at + Duration::from_secs(60));
    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, job_id)
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
    let fetched = job_mut(&mut pipeline, wedged)
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.segment_number >= 3);
    assert_eq!(fetched.len(), 97);
    let not_found_before = metrics(&pipeline)
        .articles_not_found
        .load(Ordering::Relaxed);
    let t0 = Instant::now();

    dispatch_at(&mut pipeline, t0);
    let kicked_at = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, kicked_at);
    assert_eq!(kicks(&pipeline), 1);
    // One job's bytes move after the kick; the other's never do.
    job_mut(&mut pipeline, moving).downloaded_bytes += 512;

    dispatch_at(
        &mut pipeline,
        kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER - Duration::from_secs(1),
    );
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, wedged)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );

    dispatch_at(
        &mut pipeline,
        kicked_at + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER,
    );

    assert_eq!(releases(&pipeline), 1);
    assert_eq!(kicks(&pipeline), 1);
    assert!(
        stall_job(&pipeline, wedged)
            .articles
            .active()
            .unwrap()
            .download_queue
            .is_empty()
    );
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed)
            - not_found_before,
        3
    );
    assert_eq!(stall_job(&pipeline, wedged).failed_bytes, 3 * 512);
    assert!(completion_pending(&pipeline, wedged));
    assert_eq!(
        stall_job(&pipeline, moving)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(stall_job(&pipeline, moving).failed_bytes, 0);
    assert!(!completion_pending(&pipeline, moving));
    assert!(stall_since(&pipeline).is_none());
}

#[tokio::test]
async fn a_stall_with_no_server_to_dispatch_to_waits_for_one() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = stalled_pipeline(&temp_dir).await;
    let job_id = JobId(40007);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    park_indefinite_infrastructure_retry(&mut pipeline, job_id);
    client_of(&pipeline)
        .pool()
        .health()
        .lock()
        .await
        .record_cooldown(0, weaver_nntp::CooldownReason::Transport);
    assert_eq!(
        client_of(&pipeline)
            .blocking_body_server_order(weaver_nntp::ServerSet::EMPTY)
            .map(|order| order.len()),
        Some(0)
    );
    let t0 = Instant::now();

    dispatch_at(&mut pipeline, t0);
    let past_kick = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, past_kick);
    let past_release = past_kick + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER + Duration::from_secs(60);
    dispatch_at(&mut pipeline, past_release);

    assert_eq!(last_log(&pipeline), Some(past_release));
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(infra_count(&pipeline), 1);
    assert_eq!(
        stall_job(&pipeline, job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(stall_job(&pipeline, job_id).failed_bytes, 0);
    assert!(!completion_pending(&pipeline, job_id));

    // The server comes back right after the last pass that found it out:
    // the kick window opens from there, not from the stall's start.
    client_of(&pipeline)
        .pool()
        .health()
        .lock()
        .await
        .record_success(0);
    let back_at = past_release;
    dispatch_at(&mut pipeline, back_at + Duration::from_secs(60));
    assert_eq!(kicks(&pipeline), 0);
    dispatch_at(
        &mut pipeline,
        back_at + DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
    );
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(infra_count(&pipeline), 1);

    dispatch_at(&mut pipeline, back_at + DOWNLOAD_DISPATCH_STALL_KICK_AFTER);
    assert_eq!(kicks(&pipeline), 1);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(infra_count(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, job_id)
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
    set_recovery(&mut pipeline, false);
    let job_id = JobId(40006);
    insert_stalled_job(&mut pipeline, job_id, 3).await;
    park_indefinite_infrastructure_retry(&mut pipeline, job_id);
    let t0 = Instant::now();
    durable(&mut pipeline).insert(job_id, t0);

    dispatch_at(&mut pipeline, t0);
    // One pass that opens nothing is not yet a stall.
    assert_eq!(last_log(&pipeline), None);
    assert_eq!(liveness_since(&pipeline), Some(t0));
    let logged_at = t0 + DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;
    dispatch_at(&mut pipeline, logged_at);
    assert_eq!(last_log(&pipeline), Some(logged_at));
    let kick_window = t0 + DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
    dispatch_at(&mut pipeline, kick_window);
    let past_release =
        kick_window + DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER + Duration::from_secs(60);
    dispatch_at(&mut pipeline, past_release);

    assert_eq!(last_log(&pipeline), Some(past_release));
    assert_eq!(kicks(&pipeline), 0);
    assert_eq!(releases(&pipeline), 0);
    assert!(stall_since(&pipeline).is_none());
    assert_eq!(infra_count(&pipeline), 1);
    assert_eq!(
        stall_job(&pipeline, job_id)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        2
    );
    assert_eq!(stall_job(&pipeline, job_id).failed_bytes, 0);
    assert!(durable(&mut pipeline).contains_key(&job_id));
    assert!(!completion_pending(&pipeline, job_id));
}

#[test]
fn stall_recovery_is_on_unless_the_variable_says_0_false_or_off() {
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
    set_delay(&mut pipeline, Duration::from_secs(3600));
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, wedged).await;
    let hold = hold_until(&mut pipeline, waiting).unwrap();
    verification_run_real_kick(&mut pipeline);
    assert_eq!(propagation(&mut pipeline)[&waiting].0, hold);
    let kicked = &kick(&pipeline).unwrap().1;
    assert!(kicked.contains_key(&wedged));
    assert!(!kicked.contains_key(&waiting));
    assert_eq!(
        stall_job(&pipeline, waiting)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
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
    set_delay(&mut pipeline, Duration::from_secs(3600));
    propagation(&mut pipeline).insert(waiting, (hold, 0));
    dispatch(&mut pipeline);
    assert_eq!(releases(&pipeline), 1);
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        3
    );
    assert_eq!(
        stall_job(&pipeline, waiting)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(stall_job(&pipeline, waiting).failed_bytes, 0);
    assert_eq!(propagation(&mut pipeline)[&waiting].0, hold);
}

#[tokio::test]
async fn operator_pause_and_resume_discard_pre_pause_stall_history() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = verification_pipeline(&temp, true).await;
    let job = JobId(41914);
    insert_stalled_job(&mut pipeline, job, 3).await;
    verification_exclude_healthy_and_cool_remaining(&mut pipeline, job).await;
    verification_old_kick(&mut pipeline, &[job]);
    handle(&pipeline).pause_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(stall_since(&pipeline).is_none());
    dispatch(&mut pipeline);
    assert!(stall_since(&pipeline).is_none());
    handle(&pipeline).resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    dispatch(&mut pipeline);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(kicks(&pipeline), 0);
    assert!(kick(&pipeline).is_none());
    assert_eq!(
        stall_job(&pipeline, job)
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
    set_delay(&mut pipeline, Duration::from_secs(3600));
    let ready_at = Instant::now() + Duration::from_secs(3600);
    propagation(&mut pipeline).insert(job, (ready_at, 0));
    dispatch(&mut pipeline);
    let since = stall_since(&pipeline).unwrap();
    assert!(kick(&pipeline).is_none());
    assert_eq!(since, ready_at);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        stall_job(&pipeline, job)
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
    let queue = &mut job_mut(&mut pipeline, job)
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
        stall_job(pipeline, job)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
    };

    // The kick window has passed: the clock restarts now, not at the
    // verdict instant an hour ahead.
    let now = Instant::now();
    watch_from(
        &mut pipeline,
        now - DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
    );
    dispatch(&mut pipeline);
    assert_eq!(kicks(&pipeline), 0);
    let since = stall_since(&pipeline).unwrap();
    assert!(kick(&pipeline).is_none());
    assert!(since >= now, "the clock restarts");
    assert!(
        since <= Instant::now(),
        "at the pass, not at the verdict instant ahead"
    );
    assert_eq!(
        wait_reason(&pipeline, job, login_view(&pipeline)).as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );

    // A kick of long ago and its release window passed: nothing is booked.
    verification_old_kick(&mut pipeline, &[job]);
    dispatch(&mut pipeline);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(queued(&pipeline), 3);
    assert_eq!(stall_job(&pipeline, job).status, JobStatus::Downloading);
    assert_eq!(stall_job(&pipeline, job).failed_bytes, 0);

    // Server 1 has refused the login for its whole give-up time.
    client_of(&pipeline).pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    assert!(client_of(&pipeline).pool().refuses_credentials(1));
    verification_old_kick(&mut pipeline, &[job]);
    dispatch(&mut pipeline);
    assert_eq!(releases(&pipeline), 1);
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        3
    );
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
    let active = job_mut(&mut pipeline, job).articles.active_mut().unwrap();
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
        login_view(&pipeline).waiting(),
        weaver_nntp::ServerSet::from([1])
    );
    assert_eq!(login_wait(&pipeline, job, login_view(&pipeline)), None);
    assert_eq!(wait_reason(&pipeline, job, login_view(&pipeline)), None);
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
    let view = login_view(&pipeline);
    assert_eq!(view.waiting(), weaver_nntp::ServerSet::from([1]));
    assert_eq!(login_wait(&pipeline, job, view), None);
    assert_eq!(wait_reason(&pipeline, job, view), None);
    assert_eq!(
        earliest(&pipeline, Instant::now()),
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
    let queue = &mut job_mut(&mut pipeline, job)
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
    client_of(&pipeline)
        .pool()
        .quarantine_for_transport_for_test(1);
    assert!(!client_of(&pipeline).pool().refuses_credentials(1));
    assert_eq!(
        login_view(&pipeline).waiting(),
        weaver_nntp::ServerSet::from([1])
    );

    let now = Instant::now();
    watch_from(
        &mut pipeline,
        now - DOWNLOAD_DISPATCH_STALL_KICK_AFTER - Duration::from_secs(1),
    );
    dispatch(&mut pipeline);
    assert_eq!(kicks(&pipeline), 0);
    verification_old_kick(&mut pipeline, &[job]);
    dispatch(&mut pipeline);
    assert_eq!(releases(&pipeline), 0);
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        stall_job(&pipeline, job)
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        3
    );
    assert_eq!(stall_job(&pipeline, job).failed_bytes, 0);
    assert_eq!(
        wait_reason(&pipeline, job, login_view(&pipeline)).as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );
}

// The row stops saying the job waits on the login once the server takes it.
#[tokio::test]
async fn the_stored_login_wait_clears_once_the_login_is_taken() {
    let temp = tempfile::tempdir().unwrap();
    let job = JobId(41920);
    let mut pipeline = job_waiting_on_server_1(&temp, job).await;
    client_of(&pipeline)
        .pool()
        .take_recovery_probe_for_test(1)
        .answer();
    report_holds(&mut pipeline);
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
        client_of(&pipeline)
            .pool()
            .next_login_refusal_verdict()
            .is_some(),
        "the loop has an instant to wait for"
    );
    tell_verdicts(&mut pipeline);
    assert!(!dispatch_wake(&pipeline), "the hour is not up");
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[1]))
    );

    client_of(&pipeline).pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    tell_verdicts(&mut pipeline);
    assert!(dispatch_wake(&pipeline));
    assert_eq!(
        client_of(&pipeline).pool().next_login_refusal_verdict(),
        None,
        "told once: nothing is left to wake for"
    );
    assert!(
        client_of(&pipeline)
            .pool()
            .login_refusal_verdicts_untold()
            .is_none()
    );
    report_holds(&mut pipeline);
    assert_eq!(stored_reason(&pipeline, job), None);
}

// The refusing server is removed from the pool: the row stops naming it.
#[tokio::test]
async fn the_stored_login_wait_clears_once_the_server_is_removed() {
    let temp = tempfile::tempdir().unwrap();
    let job = JobId(41922);
    let mut pipeline = job_waiting_on_server_1(&temp, job).await;
    let kept = client_of(&pipeline).pool().server_configs()[0].clone();
    let generation = generation(&pipeline) + 1;
    activate_client(
        &mut pipeline,
        weaver_nntp::NntpClient::new(weaver_nntp::client::NntpClientConfig::single(kept, 1)),
        1,
        generation,
    );
    assert!(login_view(&pipeline).waiting().is_empty());
    report_holds(&mut pipeline);
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
    let mut pipeline = audited(&temp).await;
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
    set_client(&mut pipeline, client("old"));
    let job = JobId(41923);
    insert_stalled_job(&mut pipeline, job, 3).await;
    refuse_login(&pipeline, 0).await;
    report_holds(&mut pipeline);
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[0]))
    );

    // The new pool exists; the old one is still the pool in use.
    let verdict_at = control.login_refusal_verdict_at();
    let rebuilt = client("new");
    dispatch(&mut pipeline);
    assert!(
        matches!(
            client_of(&pipeline).pool().connection_hold(0, true),
            Some(weaver_nntp::recovery::ConnectionHold::Until(_))
        ),
        "the pool of the old password is not handed the probe"
    );
    assert_eq!(connections(&pipeline), 0);
    assert_eq!(control.login_refusal_verdict_at(), verdict_at);
    report_holds(&mut pipeline);
    assert_eq!(
        stored_reason(&pipeline, job),
        Some(login_refused_name(&pipeline, &[0])),
        "a rebuild that failed here would leave the wait as it was"
    );
    let generation = generation(&pipeline) + 1;
    activate_client(&mut pipeline, rebuilt, 1, generation);
    assert_eq!(
        client_of(&pipeline).pool().connection_hold(0, true),
        Some(weaver_nntp::recovery::ConnectionHold::Probe)
    );
    assert_eq!(control.login_refusal_verdict_at(), None);
    assert!(login_view(&pipeline).waiting().is_empty());
    report_holds(&mut pipeline);
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
    let queue = &mut job_mut(&mut pipeline, job)
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
    let view = login_view(&pipeline);
    assert_eq!(
        login_wait(&pipeline, job, view),
        Some((weaver_nntp::ServerSet::from([1]), false))
    );
    assert_eq!(
        wait_reason(&pipeline, job, view).as_deref(),
        Some(login_refused_name(&pipeline, &[1]).as_str())
    );
    assert_eq!(
        earliest(&pipeline, Instant::now()),
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
    tell_verdicts(&mut pipeline);
    assert_eq!(
        (queued(&pipeline, first), queued(&pipeline, second)),
        (3, 3)
    );
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        0,
        "inside the hour nothing is booked"
    );

    client_of(&pipeline).pool().set_login_refusal_clock(
        1,
        Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    tell_verdicts(&mut pipeline);
    assert_eq!(
        (queued(&pipeline, first), queued(&pipeline, second)),
        (0, 0)
    );
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
        6
    );
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
    let pool = Arc::clone(client_of(&pipeline).pool());
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    pool.set_login_refusal_clock(1, Instant::now() + give_up);
    pool.expire_quarantine_for_test(1);
    assert!(!pool.refuses_credentials(1), "the probe is due");
    tell_verdicts(&mut pipeline);
    assert!(!dispatch_wake(&pipeline));
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
    tell_verdicts(&mut pipeline);
    assert_eq!(
        (
            queued_for_download(&pipeline, first),
            queued_for_download(&pipeline, second)
        ),
        (0, 0)
    );
    assert_eq!(
        metrics(&pipeline)
            .articles_not_found
            .load(Ordering::Relaxed),
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
    let queue = &mut job_mut(&mut pipeline, job)
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
    let pool = Arc::clone(client_of(&pipeline).pool());
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

    let handle = handle(&pipeline);
    let metrics = metrics_arc(&pipeline);
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
    let queue = &mut job_mut(&mut pipeline, job)
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
    let view = login_view(&pipeline);
    assert_eq!(
        earliest(&pipeline, Instant::now()),
        None,
        "the job is not explained by the login wait, so a release takes it"
    );
    let released = released_work(&mut pipeline, job, view);
    assert_eq!(
        released
            .iter()
            .map(|work| work.exclude_servers)
            .collect::<Vec<_>>(),
        [weaver_nntp::ServerSet::from([0, 1])]
    );
    let left = &stall_job(&pipeline, job)
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
    client_of(&pipeline)
        .pool()
        .take_recovery_probe_for_test(1)
        .answer();
    let view = login_view(&pipeline);
    assert_eq!(released_work(&mut pipeline, job, view).len(), 2);
}

include!("stall_recovery_liveness.rs");
