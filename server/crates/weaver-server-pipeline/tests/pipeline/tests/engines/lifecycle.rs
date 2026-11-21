// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's way out of the pipeline: what fails it, what holds it at its
// barriers, and the order in which its history row, its terminal event and
// its delivery land.

use weaver_server_base::events::model::PipelineEvent;
use weaver_server_composition::{
    FieldUpdate, HistoryFilter, JobHistoryRow, JobId, JobStatus, JobUpdate, SchedulerError,
};
use weaver_server_pipeline::history::{
    nzbget_state::HistoryNzbgetStateQueries, queries::HistoryQueries,
};
use weaver_server_pipeline::pipeline::tests::{
    control_fixture::{move_deferred, move_in_flight},
    engines::{
        harness::{ARTICLE, JOB, Post, Scenario, admit, collect, payload},
        lifecycle_fixture::*,
        stream_fixtures::xz,
    },
    history_fixture::{database, hold_database_writer, terminal_status, wait_for_job_event},
    queue_fixture::{apply_intents, handle as queue_handle},
    script_fixture::{queue_script_waiting, queue_scripts_admitted},
    settle_failed_releases,
    terminal_fixture::{any_move_in_flight, settle_moves},
    window_fixture::retiring,
};
use weaver_server_pipeline::{Pipeline, SchedulerHandle};

use weaver_server_composition::post_processing::model::PipelineFailureStage;
use weaver_server_composition::{
    DuplicateAdmission, DuplicateAdmissionRequest, DuplicateMode, DuplicatePolicy,
    FingerprintEvidence, JobSpec, MessageIds, SemanticCandidateSource, SemanticDuplicate,
    SemanticPromotionState, SemanticTerminalCause, SubmissionOrigin,
};
use weaver_server_pipeline::history::HistoryFailureKind;
use weaver_server_pipeline::jobs::duplicate_persistence::JobsDuplicatePersistenceAdmitDuplicateSubmissionQueries;
use weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON;
use weaver_server_pipeline::pipeline::tests::{
    dispatch_order_fixture::eligible,
    history_fixture::set_installed_status,
    insert_active_job, new_direct_pipeline,
    queue_fixture::{
        fail_job, fail_on_health, has_login_refusals, has_terminal_cause, login_refusal_words,
    },
    refuse_login, sample_nzb_zstd,
};

async fn history(pipeline: &mut Pipeline, job: JobId) -> JobHistoryRow {
    settle_failed_releases(pipeline).await;
    database(pipeline).flush_write_queue().await.unwrap();
    database(pipeline)
        .get_job_history(job.0)
        .unwrap()
        .expect("the job is archived")
}

// Why the job's history row says it failed, `None` for a job that did not.
fn failure_kind(pipeline: &Pipeline, job: JobId) -> Option<HistoryFailureKind> {
    database(pipeline).nzbget_history_states(&[job.0]).unwrap()[&job.0].failure_kind
}

// A second copy of the post `spec` names, posted again under other article
// ids and admitted after it with a lower score: it waits, parked, for the
// first to fail.
fn parked_duplicate(pipeline: &Pipeline, spec: &JobSpec) -> JobId {
    let mut copy = spec.clone();
    for (index, file) in copy.files.iter_mut().enumerate() {
        for segment in &mut file.segments {
            segment.message_ids = MessageIds::new(vec![format!(
                "copy-{index}-{}@example.com",
                segment.ordinal
            )]);
        }
    }
    let request = DuplicateAdmissionRequest {
        evidence: FingerprintEvidence::from_validated_spec(&copy, [61; 32]),
        mode: DuplicateMode::Score,
        semantic: SemanticDuplicate::from_source(&spec.name, 5),
        semantic_source: Some(SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some(format!("{}.copy.nzb", spec.name)),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: SubmissionOrigin::Api,
        idempotency: None,
        policy: DuplicatePolicy::default(),
    };
    match database(pipeline)
        .admit_duplicate_submission(&request)
        .unwrap()
    {
        DuplicateAdmission::Parked { job_id, .. } => job_id,
        outcome => panic!("the second copy must be parked, got {outcome:?}"),
    }
}

// The last server that may serve the job has refused the pool's credentials
// for its whole give-up time: the articles only it could serve are booked
// missing, not retried, and the job
// fails for want of them. Its cause is the missing articles, so its parked
// duplicate is the one promoted next, and its reason names the server.
// `servers` holds each server's retention in days; the last one refuses, and
// with two the job is aged past the first one's retention.
async fn the_last_server_refusing_credentials_fails_the_job(servers: &[u32], named: &str) {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 1))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, servers);
    let refusing = servers.len() - 1;
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    if refusing > 0 {
        // Index 0 keeps five days of retention, index 1 everything; a job
        // ten days old is index 1's alone.
        assert_eq!(age_and_recheck_excludes(&mut pipeline, job, 10), [0]);
    }
    refuse_past_give_up(&pipeline, refusing).await;

    // The first queued article is leased and answered with what the pool
    // says for it, the way a lane task answers it.
    let pending = try_refused_first_lane(&mut pipeline, job).await;
    assert_eq!(pending.count(), 1, "the refused article takes the answer");
    assert_eq!(pending.source(), Some(refusing));
    pending.deliver(&mut pipeline).await;
    // The refused article is booked; it takes the post below its health, so
    // the feed gives the post up and leaves the rest unasked, none retried.
    assert_eq!(not_found(&pipeline), 1);
    assert_eq!(
        unservable_count(&pipeline),
        1,
        "the post has no recovery: its first loss gives it up, and the file's other articles \
         are abandoned, not booked unservable"
    );
    assert_eq!(pending_retry_count(&pipeline, job), None);
    assert_eq!(infrastructure_waits(&pipeline), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;

    assert!(
        matches!(job_status(&pipeline, job), Some(JobStatus::Failed { .. })),
        "{:?}",
        job_status(&pipeline, job)
    );
    // The post's health counts the articles no server can bring, so the
    // feed gives the post up: the job fails at its download, for health.
    assert_eq!(
        failure_stage(&pipeline, job),
        Some(PipelineFailureStage::Download)
    );
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    let error = row.error_message.unwrap_or_default();
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(HistoryFailureKind::Health),
        "{error}"
    );
    assert!(!error.starts_with("no file was delivered whole"), "{error}");
    assert!(
        error.ends_with(&format!(
            "; server {named} refuses the login and was never asked for the articles no \
             other server had"
        )),
        "{error}"
    );
    let candidate = database(&pipeline)
        .semantic_candidate_snapshot(job)
        .unwrap()
        .expect("the job was admitted");
    assert_eq!(
        candidate.terminal_cause,
        Some(SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
    assert_eq!(candidate.promotion_state, SemanticPromotionState::Pending);
    // The refusing servers are let go with the job.
    assert!(!has_login_refusals(&pipeline, job));
    // The pass that hands promoted copies to the queue takes the parked one.
    let claims = database(&pipeline)
        .reconcile_semantic_promotion_claims(8)
        .unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.trigger_job_id, claim.job_id))
            .collect::<Vec<_>>(),
        [(job, duplicate)]
    );
}

#[tokio::test]
async fn a_lone_server_refusing_credentials_fails_the_job_and_promotes_its_duplicate() {
    the_last_server_refusing_credentials_fails_the_job(&[0], "retention-0.example.com:563").await;
}

#[tokio::test]
async fn the_last_of_two_servers_refusing_credentials_fails_the_job_and_promotes_its_duplicate() {
    the_last_server_refusing_credentials_fails_the_job(&[5, 0], "retention-1.example.com:563")
        .await;
}

// The job's only server refuses the login and has not yet refused it for
// its give-up time: the article its probe was refused for waits, as through
// an outage, the job shows why for the whole wait, and nothing is booked or
// failed. The login
// is then taken at a probe and the job completes.
#[tokio::test]
async fn a_lone_server_refusing_the_login_holds_the_job_waiting_until_it_takes_the_login() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 5))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, &[0]);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    let queued = queued_len(&pipeline, job);
    refuse_login(&pipeline, 0).await;
    // The pool's own answer to a lane is a wait, not a refusal naming it.
    match client(&pipeline).try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY) {
        Ok(_) => panic!("a lane opened on a quarantined server"),
        Err(error) => assert!(error.server().is_none(), "{error:?}"),
    }

    let refused = refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert_eq!(not_found(&pipeline), 0);
    assert!(terminal_count(&pipeline) == 0);
    assert!(!has_login_refusals(&pipeline, job));
    assert!(!has_terminal_cause(&pipeline, job));
    assert_eq!(infrastructure_waits(&pipeline), 1);
    assert_eq!(queued_len(&pipeline, job), queued - 1);
    assert!(!ended(&pipeline, job));
    // In the second the refused article's retry is parked, and between
    // probes once it is back in the queue, the job names the server.
    let waiting = login_refused_reason(&pipeline, 0);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(waiting.as_str())
    );
    // Nothing the job has queued is booked for the refusal.
    assert_eq!(retire_unservable(&mut pipeline, job), 0);
    assert!(!no_server_can_serve(
        &mut pipeline,
        job,
        weaver_nntp::ServerSet::EMPTY
    ));
    assert_eq!(wake_infrastructure(&mut pipeline), 1);
    assert_eq!(infrastructure_waits(&pipeline), 0);
    assert!(!has_download_wait(&pipeline, job));
    assert_eq!(queued_len(&pipeline, job), queued);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(waiting.as_str())
    );
    // A full disk holds the job whatever the servers do, so it is the
    // reason first.
    hold_for_disk(&mut pipeline, job, true);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(DISK_SPACE_WAIT_REASON)
    );
    hold_for_disk(&mut pipeline, job, false);

    // The next probe takes the login; the waiting article goes back to the
    // queue and the job completes.
    client(&pipeline)
        .pool()
        .take_recovery_probe_for_test(0)
        .answer();
    assert!(!client(&pipeline).pool().refuses_credentials(0));
    assert_eq!(queued_len(&pipeline, job), queued);
    assert_eq!(
        dispatch_wait(&pipeline, job),
        None,
        "the login is taken: the job waits on nothing"
    );
    assert!(!booked_terminal(&pipeline, refused));
    drive(&mut pipeline, &scenario, job, |_| false).await;
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "complete");
    assert_eq!(row.error_message, None);
    assert_eq!(not_found(&pipeline), 0);
}

// Both fill servers refuse the login, and the job's one article is parked
// for its refused-login retry with server 1 excluded. The job names only
// server 0, the one the parked article waits on, and names it the same way
// once the article is back in the queue.
#[tokio::test]
async fn a_parked_refused_article_names_only_the_servers_it_waits_on() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(ARTICLE, 9))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, &[0, 0]);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    refuse_login(&pipeline, 0).await;
    refuse_login(&pipeline, 1).await;
    let mut works = take_auth_work(&mut pipeline, job);
    assert_eq!(works.count(), 1);
    let excluded = weaver_nntp::ServerSet::from([1]);
    works.park_first(&mut pipeline, job, excluded);
    let only_server_0 = login_refused_reason(&pipeline, 0);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(only_server_0.as_str())
    );
    works.requeue(&mut pipeline, job);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(only_server_0.as_str())
    );
}

// The job's only server keeps refusing the login past its give-up time: the
// probe refused then is the verdict, the job's articles are booked, the job
// fails for health and its next duplicate is tried.
#[tokio::test]
async fn a_lone_server_refusing_the_login_past_its_give_up_time_fails_the_job_for_health() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 6))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, &[0]);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    refuse_login(&pipeline, 0).await;
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(terminal_count(&pipeline) == 0, "the first refusal waits");
    assert_eq!(wake_infrastructure(&mut pipeline), 1);
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(login_refused_reason(&pipeline, 0).as_str())
    );

    // The server has refused the login for its whole give-up time, and the
    // next lane for the job meets the refusal.
    client(&pipeline).pool().set_login_refusal_clock(
        0,
        std::time::Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    assert!(client(&pipeline).pool().refuses_credentials(0));
    assert_eq!(
        dispatch_wait(&pipeline, job),
        None,
        "the refusal is the verdict: the job no longer waits on it"
    );
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(not_found(&pipeline) >= 1);
    assert_eq!(infrastructure_waits(&pipeline), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(HistoryFailureKind::Health)
    );
    let claims = database(&pipeline)
        .reconcile_semantic_promotion_claims(8)
        .unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.trigger_job_id, claim.job_id))
            .collect::<Vec<_>>(),
        [(job, duplicate)]
    );
}

// The job's only server refuses the login at one probe after another. Each
// refusal inside the give-up time is a wait: the article goes back to the
// queue, nothing is booked and the job names the server throughout. The
// probe refused once the run is its give-up time old is the verdict, and
// the job fails for health.
#[tokio::test]
async fn a_series_of_refused_probes_reaches_the_verdict_at_the_give_up_time() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 7))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, &[0]);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    let queued = queued_len(&pipeline, job);
    refuse_login(&pipeline, 0).await;
    let began = std::time::Instant::now();
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    let waiting = login_refused_reason(&pipeline, 0);
    for into_the_run in [
        give_up / 4,
        give_up / 2,
        give_up - std::time::Duration::from_secs(60),
    ] {
        client(&pipeline)
            .pool()
            .set_login_refusal_clock(0, began + into_the_run);
        refused_probe_for_first_article(&mut pipeline, job, 0).await;
        assert!(!client(&pipeline).pool().refuses_credentials(0));
        assert_eq!(not_found(&pipeline), 0);
        assert!(terminal_count(&pipeline) == 0);
        assert_eq!(
            dispatch_wait(&pipeline, job).as_deref(),
            Some(waiting.as_str()),
            "parked for its retry"
        );
        assert_eq!(wake_infrastructure(&mut pipeline), 1);
        assert_eq!(queued_len(&pipeline, job), queued);
        assert_eq!(
            dispatch_wait(&pipeline, job).as_deref(),
            Some(waiting.as_str()),
            "back in the queue"
        );
    }

    client(&pipeline)
        .pool()
        .set_login_refusal_clock(0, began + give_up + std::time::Duration::from_secs(60));
    assert!(client(&pipeline).pool().refuses_credentials(0));
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(not_found(&pipeline) >= 1);
    assert_eq!(infrastructure_waits(&pipeline), 0);
    assert_eq!(dispatch_wait(&pipeline, job), None);
    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(HistoryFailureKind::Health)
    );
}

// The job's only server asks for no login at the dial, answers every BODY
// with 480 and refuses the empty login the 480 asks for: it refuses the
// account. A real lane meets the refusal, and the server is waited for as
// through an outage:
// nothing is booked and the job names the server. Past the give-up time the
// articles only it could bring are booked missing, the job fails for health
// naming the server, it holds no connection, and its duplicate is tried.
#[tokio::test]
async fn a_lone_anonymous_server_refusing_at_body_is_waited_for_then_fails_the_job() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 13))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let server = RefusingServer::bind(&mut pipeline);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    let pool = std::sync::Arc::clone(client(&pipeline).pool());
    let began = std::time::Instant::now();
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    pool.set_login_refusal_clock(0, began);

    dispatch(&mut pipeline);
    assert!(connections(&pipeline) > 0, "a lane is seated");
    run_seated_lanes(&mut pipeline).await;
    assert_eq!(
        pool.login_refusal_waits(),
        weaver_nntp::ServerSet::from([0]),
        "health hears the 480 as a refused login, waited for"
    );
    assert!(!pool.refuses_credentials(0));
    assert_eq!(not_found(&pipeline), 0);
    assert!(terminal_count(&pipeline) == 0);
    assert!(!ended(&pipeline, job));
    assert_eq!(
        dispatch_wait(&pipeline, job).as_deref(),
        Some(login_refused_reason(&pipeline, 0).as_str())
    );
    wake_infrastructure(&mut pipeline);

    // A minute short of the give-up time the refusal is still a wait.
    pool.set_login_refusal_clock(0, began + give_up - std::time::Duration::from_secs(60));
    tell_refusal_verdicts(&mut pipeline);
    assert_eq!(not_found(&pipeline), 0);
    assert!(queued_len(&pipeline, job) > 0);

    // At the give-up time the refusal is the verdict, and the articles only
    // the server could bring are booked missing.
    pool.set_login_refusal_clock(0, began + give_up);
    assert!(pool.refuses_credentials(0));
    tell_refusal_verdicts(&mut pipeline);
    assert!(not_found(&pipeline) >= 1);
    assert_eq!(queued_or_none(&pipeline, job), 0);
    assert_eq!(infrastructure_waits(&pipeline), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(connections(&pipeline), 0);
    assert!(!has_connections(&pipeline, job));
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    let error = row.error_message.unwrap_or_default();
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(HistoryFailureKind::Health),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "server 127.0.0.1:{} refuses the login",
            server.port()
        )),
        "{error}"
    );
    let claims = database(&pipeline)
        .reconcile_semantic_promotion_claims(8)
        .unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.trigger_job_id, claim.job_id))
            .collect::<Vec<_>>(),
        [(job, duplicate)]
    );
    client(&pipeline).shutdown().await;
    drop(server);
}

// Two jobs wait on the one server refusing the login. A transport failure
// then quarantines the server across the run's give-up time: the hour
// passes with nothing told and nothing booked, as through an outage. The
// login is refused again at the probe one job's article carries. That
// refusal is the verdict for both jobs: the turn that hears it books the
// other job's waiting articles too, and the verdict is told once.
#[tokio::test]
async fn a_login_refused_past_the_give_up_time_books_every_job_that_waited() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let probed = JobId(51931);
    let other = JobId(51932);
    two_jobs_on_one_server(&mut pipeline, probed, other).await;
    let queued = queued_or_none(&pipeline, other);
    assert!(queued > 0);
    refuse_login(&pipeline, 0).await;
    let began = std::time::Instant::now();
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    let pool = std::sync::Arc::clone(client(&pipeline).pool());

    pool.quarantine_for_transport_for_test(0);
    pool.set_login_refusal_clock(0, began + give_up + std::time::Duration::from_secs(60));
    assert_eq!(
        pool.next_login_refusal_verdict(),
        None,
        "an outage past the hour is waited through"
    );
    tell_refusal_verdicts(&mut pipeline);
    assert_eq!(not_found(&pipeline), 0);
    assert_eq!(queued_or_none(&pipeline, other), queued);

    // The probe after the outage is refused.
    pool.refuse_login_for_test(0, give_up);
    assert!(pool.refuses_credentials(0));
    assert!(pool.login_refusal_verdicts_untold().is_some());
    refused_probe_for_first_article(&mut pipeline, probed, 0).await;
    assert_eq!(queued_or_none(&pipeline, probed), 0);
    assert_eq!(
        queued_or_none(&pipeline, other),
        0,
        "the job no probe carried is booked in the same turn"
    );
    assert!(has_login_refusals(&pipeline, other));
    assert!(pool.login_refusal_verdicts_untold().is_none());
    assert_eq!(pool.next_login_refusal_verdict(), None, "told once");
}

// One of two jobs waiting on a server refusing the login is paused when
// the hour lands. The verdict's sweep books the other and leaves the paused
// job's articles queued. Resumed while the server still refuses, it is
// weighed by the ordinary path: the pool refuses the lane that draws its
// first article, naming the server, and its waiting articles are booked.
#[tokio::test]
async fn a_job_paused_at_the_verdict_is_left_alone_and_booked_once_resumed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let running = JobId(51933);
    let paused = JobId(51934);
    two_jobs_on_one_server(&mut pipeline, running, paused).await;
    let queued = queued_or_none(&pipeline, paused);
    assert!(queued > 0);
    refuse_login(&pipeline, 0).await;
    set_pause_intent(&pipeline, paused, true);
    assert_eq!(eligible(&pipeline), vec![running]);

    let pool = std::sync::Arc::clone(client(&pipeline).pool());
    pool.set_login_refusal_clock(
        0,
        std::time::Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    tell_refusal_verdicts(&mut pipeline);
    assert_eq!(queued_or_none(&pipeline, running), 0);
    assert!(has_login_refusals(&pipeline, running));
    assert_eq!(
        queued_or_none(&pipeline, paused),
        queued,
        "a paused job's articles are left queued"
    );
    assert!(!has_login_refusals(&pipeline, paused));
    assert!(
        pool.login_refusal_verdicts_untold().is_none(),
        "the verdict is told all the same"
    );

    set_pause_intent(&pipeline, paused, false);
    assert!(pool.refuses_credentials(0), "the server still refuses");
    assert!(eligible(&pipeline).contains(&paused));
    match client(&pipeline).try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY) {
        Ok(_) => panic!("a lane opened on a server refusing the login"),
        Err(error) => assert_eq!(error.server(), Some(weaver_nntp::ServerId(0))),
    }
    refused_probe_for_first_article(&mut pipeline, paused, 0).await;
    assert_eq!(queued_or_none(&pipeline, paused), 0);
    assert!(has_login_refusals(&pipeline, paused));
}

// Two fill servers, the second refusing the login within its give-up time:
// the articles the first server has are fetched, and the one it lacks waits
// for the refusing server, asked of it again, not booked.
#[tokio::test]
async fn an_article_only_a_server_refusing_the_login_could_bring_waits_for_it() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 8))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_two_retention_servers(&mut pipeline);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    refuse_login(&pipeline, 1).await;
    let segment_id = first_article_not_found(&mut pipeline, job).await;
    assert!(
        !booked_terminal(&pipeline, segment_id),
        "the article waits for the refusing server"
    );
    assert_eq!(not_found(&pipeline), 0);
    assert!(!has_login_refusals(&pipeline, job));
    // It is queued again, for the refusing server alone.
    let requeued = queued_excludes(&pipeline, segment_id).expect("the article is queued again");
    assert_eq!(requeued, weaver_nntp::ServerSet::from([0]));
    assert!(!no_server_can_serve(&mut pipeline, job, requeued));
    assert_eq!(retire_unservable(&mut pipeline, job), 0);

    // The other articles come from the first server; the lost one from the
    // second once it takes the login. Nothing is booked on the way.
    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert_eq!(history(&mut pipeline, job).await.status, "complete");
    assert_eq!(not_found(&pipeline), 0);
}

// A job queued behind one whose only server refuses the login is not failed
// while the refusal waits: nothing of it is booked, and its queue is whole.
#[tokio::test]
async fn a_job_queued_behind_a_server_refusing_the_login_is_not_failed() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 9))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_retention_servers(&mut pipeline, &[0]);
    let spec = scenario.spec();
    let first = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, first, spec.clone()).await;
    let mut behind_spec = spec;
    behind_spec.name = format!("{} behind", behind_spec.name);
    let behind = JobId(first.0 + 1000);
    insert_active_job(&mut pipeline, behind, behind_spec).await;
    let behind_queued = queued_len(&pipeline, behind);
    refuse_login(&pipeline, 0).await;
    refused_probe_for_first_article(&mut pipeline, first, 0).await;

    for job in [first, behind] {
        assert!(!ended(&pipeline, job), "{job:?}");
        assert!(!no_server_can_serve(
            &mut pipeline,
            job,
            weaver_nntp::ServerSet::EMPTY
        ));
        assert_eq!(retire_unservable(&mut pipeline, job), 0);
        assert!(!has_login_refusals(&pipeline, job));
    }
    assert_eq!(queued_len(&pipeline, behind), behind_queued);
    assert!(terminal_count(&pipeline) == 0);
    assert_eq!(not_found(&pipeline), 0);
}

// An article fails once on its server, with a refused login or a broken
// connection, is retried and served. The job later fails on its password:
// the failure the article was retried past is not the job's cause, the
// password is, and the next duplicate is promoted.
#[tokio::test]
async fn a_password_failure_after_an_article_served_past_a_server_failure_promotes_the_duplicate() {
    for kind in [FetchFailure::Auth, FetchFailure::EstablishedTransport] {
        let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 3))]);
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        use_two_retention_servers(&mut pipeline);
        let spec = scenario.spec();
        let job = admitted_job_id(&pipeline, &spec);
        let duplicate = parked_duplicate(&pipeline, &spec);
        insert_active_job(&mut pipeline, job, spec).await;
        mark_download_pass(&mut pipeline, job);
        let segment_id =
            fail_first_article(&mut pipeline, job, kind, "the server failed", 0, None).await;
        assert!(
            !booked_terminal(&pipeline, segment_id),
            "{kind:?}: the article is retried, not booked"
        );
        // The retry is served.
        serve_one(
            &mut pipeline,
            job,
            &scenario,
            (segment_id.file_id.file_index, segment_id.segment_number),
        )
        .await;

        let reason = format!("{}the password is wrong", PASSWORD_MARKER);
        set_installed_status(&mut pipeline, job, JobStatus::Extracting);
        fail_job(&mut pipeline, job, reason.clone());
        let (error, cause, promotion) = failed_record(&mut pipeline, job).await;
        assert_eq!(error.as_deref(), Some(reason.as_str()), "{kind:?}");
        assert_eq!(
            cause,
            Some(SemanticTerminalCause::PasswordFailure),
            "{kind:?}"
        );
        assert_eq!(promotion, SemanticPromotionState::Pending, "{kind:?}");
        let claims = database(&pipeline)
            .reconcile_semantic_promotion_claims(8)
            .unwrap();
        assert_eq!(
            claims
                .iter()
                .map(|claim| (claim.trigger_job_id, claim.job_id))
                .collect::<Vec<_>>(),
            [(job, duplicate)],
            "{kind:?}"
        );
    }
}

// An article server 0 does not hold under its first id, whose other server
// refuses the login, moves to its second id, and server 0 serves it. The job
// later fails on its password: the refusal ended no article, so the password
// is the job's cause and the next duplicate is promoted.
#[tokio::test]
async fn a_password_failure_after_an_article_moved_past_a_refusing_server_promotes_the_duplicate() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 4))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    use_two_retention_servers(&mut pipeline);
    let mut spec = scenario.spec();
    for segment in &mut spec.files[0].segments {
        let first = segment.message_ids.in_use().to_string();
        segment.message_ids = MessageIds::new(vec![first.clone(), format!("again-{first}")]);
    }
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    mark_download_pass(&mut pipeline, job);
    refuse_past_give_up(&pipeline, 1).await;
    let segment_id = fail_first_article(
        &mut pipeline,
        job,
        FetchFailure::Auth,
        "481 authentication failed",
        1,
        Some(0),
    )
    .await;
    assert!(
        !booked_terminal(&pipeline, segment_id),
        "the article moves to its next id, not booked"
    );
    assert!(!has_message_id_left(&pipeline, segment_id));
    serve_one(
        &mut pipeline,
        job,
        &scenario,
        (segment_id.file_id.file_index, segment_id.segment_number),
    )
    .await;

    let reason = format!("{}the password is wrong", PASSWORD_MARKER);
    set_installed_status(&mut pipeline, job, JobStatus::Extracting);
    fail_job(&mut pipeline, job, reason.clone());
    let (error, cause, promotion) = failed_record(&mut pipeline, job).await;
    assert_eq!(error.as_deref(), Some(reason.as_str()));
    assert_eq!(cause, Some(SemanticTerminalCause::PasswordFailure));
    assert_eq!(promotion, SemanticPromotionState::Pending);
    let claims = database(&pipeline)
        .reconcile_semantic_promotion_claims(8)
        .unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.trigger_job_id, claim.job_id))
            .collect::<Vec<_>>(),
        [(job, duplicate)]
    );
}

// What a failed job's record holds: its reason, its cause and whether a
// duplicate of it is promoted.
async fn failed_record(
    pipeline: &mut Pipeline,
    job: JobId,
) -> (
    Option<String>,
    Option<SemanticTerminalCause>,
    SemanticPromotionState,
) {
    let row = history(pipeline, job).await;
    assert_eq!(row.status, "failed");
    let candidate = database(pipeline)
        .semantic_candidate_snapshot(job)
        .unwrap()
        .expect("the job was admitted");
    (
        row.error_message,
        candidate.terminal_cause,
        candidate.promotion_state,
    )
}

// A job with an article a refusing server was never asked for, failed in
// `stage` for `reason`, against the same job with no server refusing. With
// `at_once` the refused article is booked the moment no server may serve it.
async fn failed_past_a_refusal_and_without(
    stage: JobStatus,
    reason: &str,
    at_once: bool,
) -> [(
    Option<String>,
    Option<SemanticTerminalCause>,
    SemanticPromotionState,
); 2] {
    let mut records = Vec::new();
    for refusing in [true, false] {
        let (_temp_dir, mut pipeline, _, job, _) =
            with_an_article_lost_by(refusing, at_once && refusing).await;
        assert_eq!(login_refusal_words(&pipeline, job).is_some(), refusing);
        set_installed_status(&mut pipeline, job, stage.clone());
        fail_job(&mut pipeline, job, reason.to_string());
        records.push(failed_record(&mut pipeline, job).await);
    }
    records.try_into().unwrap()
}

// A failure in `stage` for `reason` past a refusing server, the article
// booked either way, ends as it does with no server refusing: the same
// reason with nothing added, the same cause, and a duplicate promoted. The
// typed cause is the last one an article's failure recorded, the missing
// article, as it is on a job no server refuses; the history row reads a
// password failure from the reason.
async fn a_failure_past_a_refusing_server_keeps_its_own_cause_and_promotion(
    stage: JobStatus,
    reason: &str,
) {
    for at_once in [false, true] {
        let [refused, plain] =
            failed_past_a_refusal_and_without(stage.clone(), reason, at_once).await;
        assert_eq!(refused, plain, "at once: {at_once}");
        assert_eq!(refused.0.as_deref(), Some(reason));
        assert_eq!(
            refused.1,
            Some(SemanticTerminalCause::MissingArticlesOrLowHealth)
        );
        assert_eq!(refused.2, SemanticPromotionState::Pending);
    }
}

#[tokio::test]
async fn a_wrong_password_past_a_refusing_server_keeps_its_own_cause_and_promotion() {
    let reason = format!("{}the password is wrong", PASSWORD_MARKER);
    a_failure_past_a_refusing_server_keeps_its_own_cause_and_promotion(
        JobStatus::Extracting,
        &reason,
    )
    .await;
}

#[tokio::test]
async fn a_failed_repair_past_a_refusing_server_keeps_its_own_cause_and_promotion() {
    a_failure_past_a_refusing_server_keeps_its_own_cause_and_promotion(
        JobStatus::Repairing,
        "repair failed: not enough recovery blocks",
    )
    .await;
}

#[tokio::test]
async fn a_failed_unpack_past_a_refusing_server_keeps_its_own_cause_and_promotion() {
    a_failure_past_a_refusing_server_keeps_its_own_cause_and_promotion(
        JobStatus::Extracting,
        "extraction failed: the archive is damaged",
    )
    .await;
}

// The job failing for want of articles names the refusing server. Its cause
// is still the missing articles, the servers asked having said so, and a
// duplicate is promoted, as with no server refusing.
#[tokio::test]
async fn a_failure_for_want_of_articles_past_a_refusing_server_names_it() {
    let mut records = Vec::new();
    for refusing in [true, false] {
        let (_temp_dir, mut pipeline, _, job, _) = with_an_article_lost(refusing).await;
        fail_on_health(&mut pipeline, job, "the post cannot complete".to_string());
        records.push(failed_record(&mut pipeline, job).await);
    }
    assert_eq!(
        records[0],
        (
            Some(
                "the post cannot complete; server retention-1.example.com:563 refuses the login and \
                 was never asked for the articles no other server had"
                    .to_string()
            ),
            Some(SemanticTerminalCause::MissingArticlesOrLowHealth),
            SemanticPromotionState::Pending
        )
    );
    assert_eq!(
        records[1],
        (
            Some("the post cannot complete".to_string()),
            Some(SemanticTerminalCause::MissingArticlesOrLowHealth),
            SemanticPromotionState::Pending
        )
    );
}

// A job its recovery covers. One payload article is not on the server asked,
// and the other server refuses the login and is never asked: the article is
// booked unservable, the set rebuilds it, and the job completes whole, with
// no failure and no refusal in its record. Booked the moment no server may
// serve it, it is booked unservable alone and the job completes the same.
#[tokio::test]
async fn a_job_its_recovery_covers_completes_past_an_article_a_refusing_server_never_served() {
    for (at_once, not_found) in [(false, true), (true, false)] {
        let (_temp_dir, mut pipeline, scenario, job, lost) =
            with_an_article_lost_by(true, at_once).await;
        assert!(booked_as_unservable(&pipeline, lost, not_found));
        assert!(login_refusal_words(&pipeline, job).is_some());

        drive(&mut pipeline, &scenario, job, |_| false).await;

        let row = history(&mut pipeline, job).await;
        assert_eq!(row.status, "complete");
        assert_eq!(row.error_message, None);
        assert_eq!(failure_kind(&pipeline, job), None);
        assert!(!has_login_refusals(&pipeline, job));
        assert_eq!(
            database(&pipeline)
                .semantic_candidate_snapshot(job)
                .unwrap()
                .and_then(|candidate| candidate.terminal_cause),
            Some(SemanticTerminalCause::Success)
        );
        // The cause the booked article recorded goes with the job: run again
        // under the same id and failed on its password, the job's cause is
        // its own.
        assert!(!has_terminal_cause(&pipeline, job));
        insert_active_job(&mut pipeline, job, scenario.spec()).await;
        let reason = format!("{}the password is wrong", PASSWORD_MARKER);
        set_installed_status(&mut pipeline, job, JobStatus::Extracting);
        fail_job(&mut pipeline, job, reason.clone());
        let (error, cause, _) = failed_record(&mut pipeline, job).await;
        assert_eq!(error.as_deref(), Some(reason.as_str()));
        assert_eq!(cause, Some(SemanticTerminalCause::PasswordFailure));
    }
}

// A queue script subscribed to NZB_DOWNLOADED: the job's last decode brings
// it to the barrier, where it waits for the script instead of moving.
#[tokio::test]
async fn the_last_decode_holds_the_job_at_the_nzb_downloaded_barrier() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(3 * ARTICLE, 2))]);
    let (_temp_dir, mut pipeline, _) = admit_with_queue_scripts(&scenario).await;
    drive_to_downloaded_barrier(&mut pipeline, &scenario, JOB).await;

    assert!(
        queue_script_waiting(&pipeline, JOB),
        "the barrier is not held; status {:?}",
        job_status(&pipeline, JOB)
    );
    assert!(!any_move_in_flight(&pipeline));
    let Some(id) = receive_queue_admission(&mut pipeline).await else {
        panic!("expected queue admission");
    };
    assert_eq!(id, JOB);
    queue_scripts_admitted(&mut pipeline, id);
    assert_eq!(
        job_status(&pipeline, JOB),
        Some(JobStatus::AwaitingQueueScripts)
    );
}

// The job downloaded and settled, its final move started and not yet
// heard back: the moment just before it delivers.
async fn at_the_move(scenario: &Scenario) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    let (temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    drive(&mut pipeline, scenario, JOB, |pipeline| {
        move_in_flight(pipeline, JOB)
    })
    .await;
    assert!(
        move_in_flight(&pipeline, JOB),
        "the move did not start; status {:?}",
        job_status(&pipeline, JOB)
    );
    (temp_dir, pipeline, complete_dir)
}

fn delivered() -> Scenario {
    Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE, 3))])
}

// The completion event is heard only once the history row is readable: a
// subscriber that reacts to it finds the row through the history reads, and
// the job no longer listed live.
#[tokio::test]
async fn history_is_archived_before_the_job_is_announced_complete() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, complete_dir) = at_the_move(&scenario).await;
    let mut events = queue_handle(&pipeline).subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_moves(&mut pipeline).await;

    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert_eq!(files.len(), 1, "the job delivered: {:?}", files.keys());
    let mut heard = Vec::new();
    while let Ok(event) = events.try_recv() {
        heard.push(event);
    }
    assert!(
        !heard
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { .. })),
        "completion must not be announced while the history archive is still queued"
    );
    assert!(
        database(&pipeline)
            .get_job_history(JOB.0)
            .unwrap()
            .is_none()
    );

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert!(
        queue_handle(&pipeline)
            .list_jobs()
            .iter()
            .all(|info| info.job_id != JOB || terminal_status(&info.status)),
        "the job is published as finished before its terminal event"
    );
    // No flush: the row is readable as soon as the event is heard.
    assert_eq!(
        database(&pipeline)
            .get_job_history(JOB.0)
            .unwrap()
            .map(|row| row.status),
        Some("complete".to_string())
    );
    let listed = database(&pipeline)
        .list_job_history(&HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == JOB.0));
}

// A job retiring into history refuses every mutation until its archive is
// acknowledged, and none of them moves the queue's revision.
#[tokio::test]
async fn a_retiring_job_refuses_mutations_until_its_history_commits() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_moves(&mut pipeline).await;

    assert!(retiring(&pipeline, JOB));
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.resume_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle
            .update_job(
                JOB,
                JobUpdate {
                    category: FieldUpdate::Set("changed-category".into()),
                    ..Default::default()
                }
            )
            .await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_job(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_semantic_bad(JOB).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert_eq!(history(&mut pipeline, JOB).await.status, "complete");
}

// History hides a job the queue still holds, so a job retiring into history
// stays in the queue until its archive commits. Its terminal event finds it
// gone from the queue and its history row readable.
#[tokio::test]
async fn a_retiring_job_stays_in_the_queue_until_its_history_commits() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_moves(&mut pipeline).await;

    assert!(handle.queued_job_ids().contains(&JOB));
    let queued = handle
        .queue_jobs()
        .into_iter()
        .find(|job| job.job_id == JOB)
        .expect("the retiring job is in the queue");
    assert_eq!(queued.status, JobStatus::PostProcessing);

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert!(!handle.queue_jobs().iter().any(|job| job.job_id == JOB));
    assert!(!handle.queued_job_ids().contains(&JOB));
    assert!(
        database(&pipeline)
            .get_job_history(JOB.0)
            .unwrap()
            .is_some()
    );
}

// What a polling client reads of the job, by id and in the job list, and
// whether that shows it finished.
fn shown_finished(handle: &SchedulerHandle) -> bool {
    let by_id = handle
        .get_job(JOB)
        .is_ok_and(|job| terminal_status(&job.status));
    let listed = handle
        .list_jobs()
        .iter()
        .any(|job| job.job_id == JOB && terminal_status(&job.status));
    by_id || listed
}

// A job shown finished is one history can remove: its history row is
// written and the queue no longer holds it, so neither history delete
// answers that the job is still active.
fn removable(pipeline: &Pipeline, handle: &SchedulerHandle) -> bool {
    !handle.queued_job_ids().contains(&JOB)
        && database(pipeline).get_job_history(JOB.0).unwrap().is_some()
}

// Every read a polling client makes through the terminal sequence: while the
// history archive is held the job reads as being finished, never finished,
// and history refuses it; once the archive commits it reads finished, and
// NZBGet's HistoryDelete and HistoryFinalDelete both take it.
#[tokio::test]
async fn a_job_shown_finished_can_be_removed_from_history() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_moves(&mut pipeline).await;

    // Retiring, the archive queued and not committed.
    assert!(retiring(&pipeline, JOB));
    assert!(!shown_finished(&handle), "finished before history holds it");
    assert_eq!(
        handle.get_job(JOB).unwrap().status,
        JobStatus::PostProcessing
    );
    assert!(matches!(
        handle.delete_history(JOB, false).await,
        Err(SchedulerError::Conflict(_))
    ));

    // The archive commits. The queue's write that follows may or may not
    // have run when the flush answers; either way what is read holds.
    drop(release);
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(!shown_finished(&handle) || removable(&pipeline, &handle));

    // The terminal event is sent from the write that lists the job finished.
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert!(shown_finished(&handle));
    assert_eq!(handle.get_job(JOB).unwrap().status, JobStatus::Complete);
    assert!(removable(&pipeline, &handle));

    // NZBGet's HistoryDelete hides the row it archives; HistoryFinalDelete
    // erases it.
    assert_eq!(
        database(&pipeline)
            .archive_nzbget_history(&[JOB.0])
            .unwrap(),
        1
    );
    handle
        .delete_history(JOB, false)
        .await
        .expect("HistoryFinalDelete removes the finished job");
}

// The final move cannot be made: the job fails with the move's reason, and
// nothing is delivered.
#[tokio::test]
async fn a_final_move_that_fails_fails_the_job() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    // The complete directory is a file: nothing can be delivered under it.
    std::fs::remove_dir_all(&complete_dir).unwrap();
    std::fs::write(&complete_dir, b"").unwrap();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    match job_status(&pipeline, JOB) {
        Some(JobStatus::Failed { error }) => assert!(
            error.contains("failed to reserve a complete directory"),
            "unexpected failure: {error}"
        ),
        other => panic!("the job did not fail: {other:?}"),
    }
    assert!(complete_dir.is_file());
    assert_eq!(history(&mut pipeline, JOB).await.status, "failed");
}

// `weaver_files_missing_total` counts files, not articles: two articles lost
// from one file are one missing file carrying both.
#[tokio::test]
async fn a_file_that_lost_articles_counts_once_as_missing() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(6 * ARTICLE, 4)),
        Post::plain("ember.idx", payload(2 * ARTICLE, 5)),
    ])
    .lost(0, 1)
    .lost(0, 4);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Partial));
    let snapshot = missing_totals(&pipeline);
    assert_eq!(snapshot.0, 1);
    assert_eq!(snapshot.1, 2);
}

// An unprotected file delivered short keeps its failure: the job is partial,
// and its record carries the bytes it never received and the honest health
// left.
#[tokio::test]
async fn a_short_unprotected_file_keeps_an_honest_health() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(10 * ARTICLE, 6)),
        Post::plain("ember.idx", payload(10 * ARTICLE, 7)),
    ])
    .lost(0, 9);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Partial));
    let row = history(&mut pipeline, JOB).await;
    assert_eq!(row.failed_bytes, ARTICLE as u64);
    assert_eq!(row.health, 950);
}

// A dead repost beside the file it reposts: the delivered file is not
// refused over it, and the hole stays on the record, since nothing claims it.
#[tokio::test]
async fn a_dead_repost_beside_a_delivered_file_does_not_refuse_the_job() {
    let bytes = payload(4 * ARTICLE, 8);
    let scenario = Scenario::new(vec![
        Post::plain("silver.mkv", bytes.clone()),
        Post::plain("silver.mkv.1", bytes),
    ])
    .lost(1, 0)
    .lost(1, 1)
    .lost(1, 2)
    .lost(1, 3);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert!(
        matches!(
            job_status(&pipeline, JOB),
            Some(JobStatus::Complete | JobStatus::Partial)
        ),
        "{:?}",
        job_status(&pipeline, JOB)
    );
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert!(
        files.keys().any(|name| name.ends_with("silver.mkv")),
        "{:?}",
        files.keys()
    );
    let row = history(&mut pipeline, JOB).await;
    assert_eq!(row.failed_bytes, 4 * ARTICLE as u64);
    assert_eq!(row.health, 500);
}

// The job's snapshot says, file by file, what of each the download has
// still to bring: an article served leaves its file short by its size, and
// a file nothing of which came is still to come whole.
#[tokio::test]
async fn the_job_snapshot_says_what_of_each_file_is_still_to_come() {
    let scenario = Scenario::new(vec![
        Post::plain("alpha.bin", payload(4 * ARTICLE, 71)),
        Post::plain("beta.bin", payload(ARTICLE, 72)),
    ]);
    let spec = scenario.spec();
    let sizes = |file: usize| -> Vec<u64> {
        spec.files[file]
            .segments
            .iter()
            .map(|segment| u64::from(segment.bytes))
            .collect()
    };
    let (alpha, beta) = (sizes(0), sizes(1));
    assert!(alpha.len() > 1, "alpha spans several articles");
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;

    let snapshot = |pipeline: &Pipeline| {
        live_jobs(pipeline)
            .into_iter()
            .find(|job| job.job_id == JOB)
            .expect("the job is listed")
            .file_remaining_bytes
    };
    assert_eq!(
        snapshot(&pipeline),
        vec![alpha.iter().sum::<u64>(), beta.iter().sum::<u64>()]
    );

    serve_one(&mut pipeline, JOB, &scenario, (0, 0)).await;
    assert_eq!(
        snapshot(&pipeline),
        vec![alpha[1..].iter().sum::<u64>(), beta.iter().sum::<u64>()]
    );
}

// Paused post-processing holds the final move and nothing of the download:
// every file is delivered and counted while the job waits at its move, and
// the job completes once post-processing resumes.
#[tokio::test]
async fn paused_post_processing_holds_the_move_but_not_the_delivery() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(3 * ARTICLE, 81)),
        Post::plain("ember.idx", payload(ARTICLE, 82)),
    ]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let handle = queue_handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;

    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        move_deferred(pipeline, JOB)
    })
    .await;
    assert!(
        move_deferred(&pipeline, JOB),
        "the move was not held; status {:?}",
        job_status(&pipeline, JOB)
    );
    let info = live_jobs(&pipeline)
        .into_iter()
        .find(|job| job.job_id == JOB)
        .expect("the held job is still listed");
    assert_eq!(info.total_files, 2);
    assert_eq!(info.completed_files, info.total_files);
    assert!(!ended(&pipeline, JOB));

    handle.resume_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    for name in ["ember.bin", "ember.idx"] {
        assert!(
            files.keys().any(|path| path.ends_with(name)),
            "{name} not delivered: {:?}",
            files.keys()
        );
    }
}

// The record a graph job leaves when it completes counts every file it
// brought whole, as the live report did.
#[tokio::test]
async fn the_history_record_of_a_complete_graph_job_counts_every_file() {
    let scenario = Scenario::new(vec![
        Post::plain("ember.bin", payload(3 * ARTICLE, 83)),
        Post::plain("ember.idx", payload(ARTICLE, 84)),
    ]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let mut events = queue_handle(&pipeline).subscribe_events();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;

    let finished = queue_handle(&pipeline)
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == JOB)
        .expect("the finished job is listed");
    assert_eq!(finished.status, JobStatus::Complete);
    assert_eq!(finished.total_files, 2);
    assert_eq!(finished.completed_files, finished.total_files);
}

// A pause and a resume of post-processing that both land before the
// pipeline's next intent turn hold nothing: the pause was never applied, so
// no run of the job waits on a resume walk that never comes, and the job
// completes.
#[tokio::test]
async fn a_pause_and_resume_between_intent_turns_leaves_no_run_held() {
    let body = payload(3 * ARTICLE + 7, 85);
    let scenario = Scenario::new(vec![Post::plain("ember.bin.xz", xz(&body))]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let handle = queue_handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    // Every article decoded into the graph while the pause is written and
    // not yet applied: the stream's run is asked for in that window.
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        graph_bytes_drained(pipeline, JOB)
    })
    .await;
    handle.resume_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(job_status(&pipeline, JOB), Some(JobStatus::Complete));
    let mut files = std::collections::BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    assert!(
        files
            .iter()
            .any(|(name, bytes)| name.ends_with("ember.bin") && *bytes == body),
        "{:?}",
        files.keys()
    );
}

// The terminal event a job ends with once its terminal post-processing has
// finished, and every event heard for it before that one.
async fn terminal_after_post_processing(
    pipeline: &mut Pipeline,
    job: JobId,
) -> (PipelineEvent, Vec<PipelineEvent>) {
    use weaver_server_pipeline::pipeline::tests::history_fixture::complete_after_post_processing;

    let mut events = queue_handle(pipeline).subscribe_events();
    complete_after_post_processing(pipeline, job);
    let mut heard = Vec::new();
    loop {
        let event = events.recv().await.expect("pipeline event channel closed");
        if matches!(
            &event,
            PipelineEvent::JobCompleted { job_id } | PipelineEvent::JobFailed { job_id, .. }
                if *job_id == job
        ) {
            return (event, heard);
        }
        heard.push(event);
    }
}

// A job its graph rules Failed when its post-processing finishes, its one
// file never delivered, is announced failed with its history row's reason,
// never completed.
#[tokio::test]
async fn a_job_ruled_failed_after_post_processing_is_announced_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job = JobId(9301);
    insert_active_job(
        &mut pipeline,
        job,
        weaver_server_pipeline::pipeline::tests::segmented_job_spec(
            "never delivered",
            "payload.bin",
            &[100],
        ),
    )
    .await;
    let (terminal, heard) = terminal_after_post_processing(&mut pipeline, job).await;

    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    match terminal {
        PipelineEvent::JobFailed { job_id, error } => {
            assert_eq!(job_id, job);
            assert_eq!(Some(error), row.error_message);
        }
        other => panic!("a failed job was announced as {other:?}"),
    }
    assert!(
        !heard
            .iter()
            .any(|event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == job)),
        "a failed job was also announced complete"
    );
}

// A job delivered whole is still announced completed on the same path, and
// never failed.
#[tokio::test]
async fn a_job_ruled_complete_after_post_processing_is_announced_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job = JobId(9302);
    weaver_server_pipeline::pipeline::tests::insert_delivered_job(
        &mut pipeline,
        job,
        "delivered whole",
        "delivered.mkv",
        b"delivered payload",
    )
    .await;
    let (terminal, heard) = terminal_after_post_processing(&mut pipeline, job).await;

    assert_eq!(history(&mut pipeline, job).await.status, "complete");
    assert!(
        matches!(terminal, PipelineEvent::JobCompleted { job_id } if job_id == job),
        "a complete job was announced as {terminal:?}"
    );
    assert!(
        !heard.iter().any(
            |event| matches!(event, PipelineEvent::JobFailed { job_id, .. } if *job_id == job)
        ),
        "a complete job was also announced failed"
    );
}

// A job's terminal event waits for the subscriber that keeps the job event
// rows: it is not heard until that subscriber has answered for it.
#[tokio::test]
async fn the_terminal_event_waits_for_the_job_event_rows() {
    let scenario = Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE, 88))]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let mut settles = database(&pipeline).keep_job_event_rows();
    let mut events = queue_handle(&pipeline).subscribe_events();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    let waiting = settles
        .recv()
        .await
        .expect("the terminal event waits for its rows");
    assert!(matches!(waiting.terminal, PipelineEvent::JobCompleted { job_id } if job_id == JOB));
    while let Ok(event) = events.try_recv() {
        assert!(
            !matches!(event, PipelineEvent::JobCompleted { job_id } if job_id == JOB),
            "the terminal event was heard before its rows were answered for"
        );
    }
    waiting.written.send(()).unwrap();
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
}
