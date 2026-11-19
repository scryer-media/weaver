// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's way out of the pipeline: what fails it, what holds it at its
// barriers, and the order in which its history row, its terminal event and
// its delivery land.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use super::super::restore_history::{hold_database_writer, wait_for_job_event};
use super::super::script_events::enable_queue_script;
use super::super::{
    age_job_days, insert_active_job, job_status_for_assert, new_direct_pipeline, refuse_login,
    refuse_login_past_give_up, retention_client, sample_nzb_zstd, settle_inflight_moves,
    two_server_retention_client,
};
use super::harness::{
    ARTICLE, JOB, Post, Scenario, admit, admit_with, collect, par2_set, payload, serve_job,
};
use super::streams::xz;
use crate::jobs::ids::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::{
    DownloadBatchLease, DownloadLaneMode, DownloadResult, OwnedDownloadLaneEvent, Pipeline,
    PipelineEvent, TerminalPostProcessingEvent, is_terminal_status,
};
use crate::{SchedulerError, SchedulerHandle};

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

fn ended(pipeline: &Pipeline, job: JobId) -> bool {
    matches!(
        job_status_for_assert(pipeline, job),
        Some(JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }) | None
    )
}

// Plays the server for `job` and runs its completion gate, a run's mail and
// the moves it starts, as the orchestrator does, until `until` holds or the
// job has ended. A round that changes nothing twice over is a stall.
pub(super) async fn drive(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    job: JobId,
    until: impl Fn(&Pipeline) -> bool,
) {
    let look = |pipeline: &Pipeline| {
        let queued = pipeline.jobs.get(&job).map(|state| {
            (
                state.articles.active().unwrap().download_queue.len(),
                state.articles.active().unwrap().recovery_queue.len(),
                state.status.clone(),
            )
        });
        format!(
            "{queued:?} {} {}",
            pipeline.inflight_moves.contains(&job),
            pipeline.pending_completion_checks.len()
        )
    };
    let mut stalled = None;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve_job(pipeline, job, scenario, article).await;
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        let before = look(pipeline);
        pipeline.check_job_completion(job).await;
        pipeline.pump_decode_queue();
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pipeline.pump_decode_queue();
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        if pipeline.inflight_moves.contains(&job) {
            settle_inflight_moves(pipeline).await;
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&job)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let after = look(pipeline);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!("the job stalled: {after}");
        }
    }
}

// The job's history row, once a failed job's working directory is
// released and every queued write has landed.
async fn history(pipeline: &mut Pipeline, job: JobId) -> crate::JobHistoryRow {
    super::super::settle_failed_releases(pipeline).await;
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline
        .db
        .get_job_history(job.0)
        .unwrap()
        .expect("the job is archived")
}

// Why the job's history row says it failed, `None` for a job that did not.
fn failure_kind(pipeline: &Pipeline, job: JobId) -> Option<crate::history::HistoryFailureKind> {
    pipeline.db.nzbget_history_states(&[job.0]).unwrap()[&job.0].failure_kind
}

// Admit the job the way a submission is admitted, so its durable candidate
// row records the terminal cause the archive writes.
fn admitted_job_id(pipeline: &Pipeline, spec: &crate::jobs::model::JobSpec) -> JobId {
    let request = crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(spec, [60; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source(&spec.name, 10),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some(format!("{}.nzb", spec.name)),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: crate::SubmissionOrigin::Api,
        idempotency: None,
        policy: crate::DuplicatePolicy::default(),
    };
    let crate::DuplicateAdmission::Accepted { job_id, .. } =
        pipeline.db.admit_duplicate_submission(&request).unwrap()
    else {
        panic!("the job must be admitted");
    };
    job_id
}

// A second copy of the post `spec` names, posted again under other article
// ids and admitted after it with a lower score: it waits, parked, for the
// first to fail.
fn parked_duplicate(pipeline: &Pipeline, spec: &crate::jobs::model::JobSpec) -> JobId {
    let mut copy = spec.clone();
    for (index, file) in copy.files.iter_mut().enumerate() {
        for segment in &mut file.segments {
            segment.message_ids = crate::jobs::MessageIds::new(vec![format!(
                "copy-{index}-{}@example.com",
                segment.ordinal
            )]);
        }
    }
    let request = crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(&copy, [61; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source(&spec.name, 5),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some(format!("{}.copy.nzb", spec.name)),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: crate::SubmissionOrigin::Api,
        idempotency: None,
        policy: crate::DuplicatePolicy::default(),
    };
    match pipeline.db.admit_duplicate_submission(&request).unwrap() {
        crate::DuplicateAdmission::Parked { job_id, .. } => job_id,
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
    pipeline.nntp = std::sync::Arc::new(super::super::retention_client(servers));
    let refusing = servers.len() - 1;
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    if refusing > 0 {
        // Index 0 keeps five days of retention, index 1 everything; a job
        // ten days old is index 1's alone.
        age_job_days(&mut pipeline, job, 10);
        pipeline.clear_job_retention_excludes(job);
        assert_eq!(pipeline.job_retention_excludes(job), [0]);
    }
    refuse_login_past_give_up(&pipeline, refusing).await;

    // The first queued article is leased and answered with what the pool
    // says for it, the way a lane task answers it.
    let work = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = pipeline.dispatched(work);
    pipeline.hold_test_lane_works(vec![work.clone()], false);
    let effective_exclude_servers = pipeline.effective_exclude_servers(job, work.exclude_servers);
    let Err(error) = pipeline
        .nntp
        .try_acquire_blocking_body_lane(&[], effective_exclude_servers)
    else {
        panic!("a lane opened with no server that may serve the article");
    };
    let lease = DownloadBatchLease {
        body_activity: std::sync::Arc::default(),
        lane_id: 0,
        job_id: job,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: false,
        effective_exclude_servers,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
        works: vec![work],
    };
    pipeline.active_download_connections = 1;
    pipeline.active_download_passes.insert(job);
    pipeline.active_download_connections_by_job.insert(job, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    let mut pending: VecDeque<DownloadResult> = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed { lease, error },
        &mut pending,
    );
    assert_eq!(pending.len(), 1, "the refused article takes the answer");
    assert_eq!(pending[0].source_server_idx, Some(refusing));
    for result in pending {
        pipeline.process_released_download_done(result).await;
    }
    // The refused article is booked; it takes the post below its health, so
    // the feed gives the post up and leaves the rest unasked, none retried.
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline
            .segment_terminal_states
            .values()
            .filter(|state| **state == crate::pipeline::SegmentTerminalState::Unservable)
            .count(),
        1,
        "the post has no recovery: its first loss gives it up, and the file's other articles \
         are abandoned, not booked unservable"
    );
    assert_eq!(pipeline.pending_retries_by_job.get(&job).copied(), None);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Failed { .. })
        ),
        "{:?}",
        job_status_for_assert(&pipeline, job)
    );
    // The post's health counts the articles no server can bring, so the
    // feed gives the post up: the job fails at its download, for health.
    assert_eq!(
        pipeline.pipeline_failure_stages.get(&job).copied(),
        Some(crate::post_processing::model::PipelineFailureStage::Download)
    );
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    let error = row.error_message.unwrap_or_default();
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(crate::history::HistoryFailureKind::Health),
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
    let candidate = pipeline
        .db
        .semantic_candidate_snapshot(job)
        .unwrap()
        .expect("the job was admitted");
    assert_eq!(
        candidate.terminal_cause,
        Some(crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth)
    );
    assert_eq!(
        candidate.promotion_state,
        crate::jobs::duplicate_persistence::SemanticPromotionState::Pending
    );
    // The refusing servers are let go with the job.
    assert!(!pipeline.login_refusals.contains_key(&job));
    // The pass that hands promoted copies to the queue takes the parked one.
    let claims = pipeline.db.reconcile_semantic_promotion_claims(8).unwrap();
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

// How many articles of `job` are queued for download.
fn queued_len(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline.jobs[&job]
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

// The first queued article of `job` is leased and its lane's dial meets
// server `server` refusing the login, the way a refused probe answers it.
async fn refused_probe_for_first_article(
    pipeline: &mut Pipeline,
    job: JobId,
    server: usize,
) -> crate::pipeline::SegmentId {
    let work = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = pipeline.dispatched(work);
    let segment_id = work.segment_id;
    pipeline.hold_test_lane_works(vec![work.clone()], false);
    let effective_exclude_servers = pipeline.effective_exclude_servers(job, work.exclude_servers);
    let lease = DownloadBatchLease {
        body_activity: std::sync::Arc::default(),
        lane_id: 0,
        job_id: job,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: false,
        effective_exclude_servers,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
        works: vec![work],
    };
    pipeline.active_download_connections = 1;
    pipeline.active_download_passes.insert(job);
    pipeline.active_download_connections_by_job.insert(job, 1);
    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    let error = weaver_nntp::client::BlockingBodyLaneAcquireError::CredentialsRefused {
        server: weaver_nntp::ServerId(server),
        error: weaver_nntp::NntpError::AuthenticationFailed,
    };
    let mut pending: VecDeque<DownloadResult> = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed { lease, error },
        &mut pending,
    );
    for result in pending {
        pipeline.process_released_download_done(result).await;
    }
    segment_id
}

// The wait reason a job shows while it waits on pool server `server`
// refusing the login: the code and the server as `host:port`.
fn login_refused_reason(pipeline: &Pipeline, server: usize) -> String {
    let identity = &pipeline.nntp.pool().server_identities()[server];
    format!(
        "{}:{}:{}",
        crate::jobs::handle::LOGIN_REFUSED_WAIT_REASON,
        identity.host,
        identity.port
    )
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
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    let queued = queued_len(&pipeline, job);
    refuse_login(&pipeline, 0).await;
    // The pool's own answer to a lane is a wait, not a refusal naming it.
    match pipeline
        .nntp
        .try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY)
    {
        Ok(_) => panic!("a lane opened on a quarantined server"),
        Err(error) => assert!(error.server().is_none(), "{error:?}"),
    }

    let refused = refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert!(pipeline.segment_terminal_states.is_empty());
    assert!(!pipeline.login_refusals.contains_key(&job));
    assert!(!pipeline.semantic_terminal_causes.contains_key(&job));
    assert_eq!(pipeline.infrastructure_retries.len(), 1);
    assert_eq!(queued_len(&pipeline, job), queued - 1);
    assert!(!ended(&pipeline, job));
    // In the second the refused article's retry is parked, and between
    // probes once it is back in the queue, the job names the server.
    let waiting = login_refused_reason(&pipeline, 0);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(waiting.as_str())
    );
    // Nothing the job has queued is booked for the refusal.
    assert_eq!(pipeline.retire_unservable_queued_work(job), 0);
    assert!(!pipeline.no_server_can_serve(job, weaver_nntp::ServerSet::EMPTY));
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert!(!pipeline.download_wait_by_job.contains_key(&job));
    assert_eq!(queued_len(&pipeline, job), queued);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(waiting.as_str())
    );
    // A full disk holds the job whatever the servers do, so it is the
    // reason first.
    pipeline.disk_waiting.insert(job);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    pipeline.disk_waiting.remove(&job);

    // The next probe takes the login; the waiting article goes back to the
    // queue and the job completes.
    pipeline
        .nntp
        .pool()
        .take_recovery_probe_for_test(0)
        .answer();
    assert!(!pipeline.nntp.pool().refuses_credentials(0));
    assert_eq!(queued_len(&pipeline, job), queued);
    assert_eq!(
        pipeline.dispatch_wait_reason(job, pipeline.login_refusal_view()),
        None,
        "the login is taken: the job waits on nothing"
    );
    assert!(!pipeline.segment_terminal_states.contains_key(&refused));
    drive(&mut pipeline, &scenario, job, |_| false).await;
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "complete");
    assert_eq!(row.error_message, None);
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
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
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    refuse_login(&pipeline, 0).await;
    refuse_login(&pipeline, 1).await;
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut works = queue.drain_all();
    assert_eq!(works.len(), 1);
    let excluded = weaver_nntp::ServerSet::from([1]);
    works[0].exclude_servers = excluded;
    pipeline.download_wait_by_job.insert(
        job,
        crate::pipeline::DownloadWaitStatus {
            reason: crate::pipeline::AUTH_WAIT_REASON,
            retry_at_epoch_ms: None,
            pending_count: 1,
            exclude_servers: excluded,
        },
    );
    let only_server_0 = login_refused_reason(&pipeline, 0);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(only_server_0.as_str())
    );
    pipeline.download_wait_by_job.remove(&job);
    let queue = &mut pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    for work in works {
        queue.push(work);
    }
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
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
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    refuse_login(&pipeline, 0).await;
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(
        pipeline.segment_terminal_states.is_empty(),
        "the first refusal waits"
    );
    assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(login_refused_reason(&pipeline, 0).as_str())
    );

    // The server has refused the login for its whole give-up time, and the
    // next lane for the job meets the refusal.
    pipeline.nntp.pool().set_login_refusal_clock(
        0,
        std::time::Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    assert!(pipeline.nntp.pool().refuses_credentials(0));
    assert_eq!(
        pipeline.dispatch_wait_reason(job, pipeline.login_refusal_view()),
        None,
        "the refusal is the verdict: the job no longer waits on it"
    );
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(pipeline.metrics.articles_not_found.load(Ordering::Relaxed) >= 1);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(crate::history::HistoryFailureKind::Health)
    );
    let claims = pipeline.db.reconcile_semantic_promotion_claims(8).unwrap();
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
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
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
        pipeline
            .nntp
            .pool()
            .set_login_refusal_clock(0, began + into_the_run);
        refused_probe_for_first_article(&mut pipeline, job, 0).await;
        assert!(!pipeline.nntp.pool().refuses_credentials(0));
        assert_eq!(
            pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
            0
        );
        assert!(pipeline.segment_terminal_states.is_empty());
        assert_eq!(
            pipeline
                .dispatch_wait_reason(job, pipeline.login_refusal_view())
                .as_deref(),
            Some(waiting.as_str()),
            "parked for its retry"
        );
        assert_eq!(pipeline.wake_all_infrastructure_retries(), 1);
        assert_eq!(queued_len(&pipeline, job), queued);
        assert_eq!(
            pipeline
                .dispatch_wait_reason(job, pipeline.login_refusal_view())
                .as_deref(),
            Some(waiting.as_str()),
            "back in the queue"
        );
    }

    pipeline
        .nntp
        .pool()
        .set_login_refusal_clock(0, began + give_up + std::time::Duration::from_secs(60));
    assert!(pipeline.nntp.pool().refuses_credentials(0));
    refused_probe_for_first_article(&mut pipeline, job, 0).await;
    assert!(pipeline.metrics.articles_not_found.load(Ordering::Relaxed) >= 1);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);
    assert_eq!(
        pipeline.dispatch_wait_reason(job, pipeline.login_refusal_view()),
        None
    );
    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(crate::history::HistoryFailureKind::Health)
    );
}

// A news server on this machine that asks for no login at the dial, takes
// every connection, answers every BODY with 480 and refuses the empty
// login that 480 asks for.
fn anonymous_server_refusing_at_body() -> super::harness::Listener {
    super::harness::Listener::serve(|socket| {
        use std::io::{BufRead, Write};
        let mut writer = socket.try_clone().unwrap();
        if writer.write_all(b"200 ready\r\n").is_err() {
            return;
        }
        for line in std::io::BufReader::new(socket).lines() {
            let Ok(line) = line else { break };
            let upper = line.to_ascii_uppercase();
            let reply: &[u8] = if upper == "CAPABILITIES" {
                b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
            } else if upper.starts_with("GROUP ") {
                b"211 1 1 1 alt.binaries.test\r\n"
            } else if upper.starts_with("BODY ") {
                b"480 authentication required\r\n"
            } else if upper.starts_with("AUTHINFO USER") {
                b"381 password\r\n"
            } else if upper.starts_with("AUTHINFO PASS") {
                b"481 authentication failed\r\n"
            } else if upper == "QUIT" {
                let _ = writer.write_all(b"205 closing\r\n");
                break;
            } else {
                b"500 command not recognized\r\n"
            };
            if writer.write_all(reply).is_err() {
                break;
            }
        }
    })
}

// Runs the lanes the last dispatch seated as the runtime's loop runs them,
// until every lane has parked and given its connection back.
async fn run_seated_lanes(pipeline: &mut Pipeline) {
    let mut completed = false;
    while !completed || pipeline.active_download_connections > 0 {
        tokio::select! {
            Some(event) = pipeline.owned_download_lane_event_rx.recv() => {
                completed |= matches!(event, OwnedDownloadLaneEvent::BatchComplete { .. });
                let mut pending: VecDeque<DownloadResult> = VecDeque::new();
                pipeline.handle_owned_download_lane_event(event, &mut pending);
                for result in pending {
                    pipeline.process_released_download_done(result).await;
                }
            }
            Some(request) = pipeline.download_refill_rx.recv() => {
                let _ = request.response_tx.send(crate::pipeline::DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: crate::pipeline::download::LaneParkReason::NoWork,
                });
            }
            Some(parked) = pipeline.download_lane_parked_rx.recv() => {
                pipeline.handle_download_lane_parked(parked);
            }
        }
    }
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
    let server = anonymous_server_refusing_at_body();
    let mut config = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port: server.port,
        tls: false,
        ..Default::default()
    });
    config.max_connections = 1;
    pipeline.nntp = std::sync::Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![config],
            max_idle_age: std::time::Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: std::time::Duration::from_secs(3600),
        },
    ));
    pipeline.tuner.set_connection_limit(1);
    pipeline.owned_download_lane_pool.resize(1);
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    let pool = std::sync::Arc::clone(pipeline.nntp.pool());
    let began = std::time::Instant::now();
    let give_up = weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP;
    pool.set_login_refusal_clock(0, began);

    pipeline.dispatch_downloads();
    assert!(pipeline.active_download_connections > 0, "a lane is seated");
    run_seated_lanes(&mut pipeline).await;
    assert_eq!(
        pool.login_refusal_waits(),
        weaver_nntp::ServerSet::from([0]),
        "health hears the 480 as a refused login, waited for"
    );
    assert!(!pool.refuses_credentials(0));
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert!(pipeline.segment_terminal_states.is_empty());
    assert!(!ended(&pipeline, job));
    assert_eq!(
        pipeline
            .dispatch_wait_reason(job, pipeline.login_refusal_view())
            .as_deref(),
        Some(login_refused_reason(&pipeline, 0).as_str())
    );
    pipeline.wake_all_infrastructure_retries();

    // A minute short of the give-up time the refusal is still a wait.
    pool.set_login_refusal_clock(0, began + give_up - std::time::Duration::from_secs(60));
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert!(queued_len(&pipeline, job) > 0);

    // At the give-up time the refusal is the verdict, and the articles only
    // the server could bring are booked missing.
    pool.set_login_refusal_clock(0, began + give_up);
    assert!(pool.refuses_credentials(0));
    pipeline.tell_login_refusal_verdicts();
    assert!(pipeline.metrics.articles_not_found.load(Ordering::Relaxed) >= 1);
    assert_eq!(queued_or_none(&pipeline, job), 0);
    assert_eq!(pipeline.infrastructure_retries.len(), 0);

    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(pipeline.active_download_connections, 0);
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&job)
    );
    let row = history(&mut pipeline, job).await;
    assert_eq!(row.status, "failed");
    let error = row.error_message.unwrap_or_default();
    assert_eq!(
        failure_kind(&pipeline, job),
        Some(crate::history::HistoryFailureKind::Health),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "server 127.0.0.1:{} refuses the login",
            server.port
        )),
        "{error}"
    );
    let claims = pipeline.db.reconcile_semantic_promotion_claims(8).unwrap();
    assert_eq!(
        claims
            .iter()
            .map(|claim| (claim.trigger_job_id, claim.job_id))
            .collect::<Vec<_>>(),
        [(job, duplicate)]
    );
    pipeline.nntp.shutdown().await;
    drop(server);
}

// Two jobs whose only server is pool server 0, installed and downloading.
async fn two_jobs_on_one_server(pipeline: &mut Pipeline, first: JobId, second: JobId) {
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    for (job, name, seed) in [(first, "first.bin", 11), (second, "second.bin", 12)] {
        let mut spec = Scenario::new(vec![Post::plain(name, payload(4 * ARTICLE, seed))]).spec();
        spec.name = format!("Waiting {}", job.0);
        insert_active_job(pipeline, job, spec).await;
        pipeline.active_download_passes.insert(job);
    }
}

// How many articles of `job` are queued for download, none once the job
// has nothing installed to queue.
fn queued_or_none(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline
        .jobs
        .get(&job)
        .and_then(|state| state.articles.active())
        .map_or(0, |active| active.download_queue.len())
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
    let pool = std::sync::Arc::clone(pipeline.nntp.pool());

    pool.quarantine_for_transport_for_test(0);
    pool.set_login_refusal_clock(0, began + give_up + std::time::Duration::from_secs(60));
    assert_eq!(
        pool.next_login_refusal_verdict(),
        None,
        "an outage past the hour is waited through"
    );
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
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
    assert!(pipeline.login_refusals.contains_key(&other));
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
    let queue = pipeline.shared_state.queue.clone();
    queue.set_job_paused(paused, true).unwrap();
    assert_eq!(pipeline.download_scheduler_eligible_jobs(), vec![running]);

    let pool = std::sync::Arc::clone(pipeline.nntp.pool());
    pool.set_login_refusal_clock(
        0,
        std::time::Instant::now() + weaver_nntp::health::LOGIN_REFUSAL_GIVE_UP,
    );
    pipeline.tell_login_refusal_verdicts();
    assert_eq!(queued_or_none(&pipeline, running), 0);
    assert!(pipeline.login_refusals.contains_key(&running));
    assert_eq!(
        queued_or_none(&pipeline, paused),
        queued,
        "a paused job's articles are left queued"
    );
    assert!(!pipeline.login_refusals.contains_key(&paused));
    assert!(
        pool.login_refusal_verdicts_untold().is_none(),
        "the verdict is told all the same"
    );

    queue.set_job_paused(paused, false).unwrap();
    assert!(pool.refuses_credentials(0), "the server still refuses");
    assert!(
        pipeline
            .download_scheduler_eligible_jobs()
            .contains(&paused)
    );
    match pipeline
        .nntp
        .try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY)
    {
        Ok(_) => panic!("a lane opened on a server refusing the login"),
        Err(error) => assert_eq!(error.server(), Some(weaver_nntp::ServerId(0))),
    }
    refused_probe_for_first_article(&mut pipeline, paused, 0).await;
    assert_eq!(queued_or_none(&pipeline, paused), 0);
    assert!(pipeline.login_refusals.contains_key(&paused));
}

// Two fill servers, the second refusing the login within its give-up time:
// the articles the first server has are fetched, and the one it lacks waits
// for the refusing server, asked of it again, not booked.
#[tokio::test]
async fn an_article_only_a_server_refusing_the_login_could_bring_waits_for_it() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 8))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    refuse_login(&pipeline, 1).await;
    let mut queued = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    let lost = queued.remove(0);
    for work in queued {
        pipeline
            .jobs
            .get_mut(&job)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
    let lost = pipeline.dispatched(lost);
    let (segment_id, retry_count, exclude_servers) =
        (lost.segment_id, lost.retry_count, lost.exclude_servers);
    pipeline.hold_test_lane_works(vec![lost], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: job,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(crate::pipeline::DownloadError::fetch(
                crate::pipeline::DownloadFailureKind::ArticleNotFound,
                "430 no such article",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: crate::pipeline::DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
    assert!(
        !pipeline.segment_terminal_states.contains_key(&segment_id),
        "the article waits for the refusing server"
    );
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
    assert!(!pipeline.login_refusals.contains_key(&job));
    // It is queued again, for the refusing server alone.
    let requeued = pipeline.jobs[&job]
        .articles
        .active()
        .unwrap()
        .download_queue
        .peek_first_matching(|work| work.segment_id == segment_id)
        .map(|work| work.exclude_servers)
        .expect("the article is queued again");
    assert_eq!(requeued, weaver_nntp::ServerSet::from([0]));
    assert!(!pipeline.no_server_can_serve(job, requeued));
    assert_eq!(pipeline.retire_unservable_queued_work(job), 0);

    // The other articles come from the first server; the lost one from the
    // second once it takes the login. Nothing is booked on the way.
    drive(&mut pipeline, &scenario, job, |_| false).await;
    assert_eq!(history(&mut pipeline, job).await.status, "complete");
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
}

// A job queued behind one whose only server refuses the login is not failed
// while the refusal waits: nothing of it is booked, and its queue is whole.
#[tokio::test]
async fn a_job_queued_behind_a_server_refusing_the_login_is_not_failed() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 9))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
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
        assert!(!pipeline.no_server_can_serve(job, weaver_nntp::ServerSet::EMPTY));
        assert_eq!(pipeline.retire_unservable_queued_work(job), 0);
        assert!(!pipeline.login_refusals.contains_key(&job));
    }
    assert_eq!(queued_len(&pipeline, behind), behind_queued);
    assert!(pipeline.segment_terminal_states.is_empty());
    assert_eq!(
        pipeline.metrics.articles_not_found.load(Ordering::Relaxed),
        0
    );
}

// A job whose recovery covers a lost article, with the payload's second
// article answered "not found" by server 0. Server 1 refuses the login when
// `refusing`, and is never asked; otherwise it answers "not found" too.
async fn with_an_article_lost(
    refusing: bool,
) -> (
    tempfile::TempDir,
    Pipeline,
    Scenario,
    JobId,
    crate::pipeline::SegmentId,
) {
    with_an_article_lost_by(refusing, false).await
}

// As [`with_an_article_lost`]; `at_once` books the article the moment no
// server may serve it, server 0 having already said "not found" and server 1
// refusing the login, as the queued work no server may fetch is booked.
async fn with_an_article_lost_by(
    refusing: bool,
    at_once: bool,
) -> (
    tempfile::TempDir,
    Pipeline,
    Scenario,
    JobId,
    crate::pipeline::SegmentId,
) {
    let data = payload(4 * ARTICLE, 7);
    let mut posts = vec![Post::plain("payload.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("payload.bin", &data)], 2));
    let scenario = Scenario::new(posts);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // Neither server's retention rules out a job posted now.
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let spec = scenario.spec();
    let job = admitted_job_id(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    if refusing {
        refuse_login_past_give_up(&pipeline, 1).await;
    }

    // The payload's second article is taken off the queue and answered "not
    // found" by server 0, the way a lane answers it.
    let lost = crate::pipeline::SegmentId {
        file_id: crate::jobs::ids::NzbFileId {
            job_id: job,
            file_index: 0,
        },
        segment_number: 1,
    };
    let queued = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    let mut work = None;
    for queued in pipeline.dispatched_all(queued) {
        if queued.segment_id == lost {
            work = Some(queued);
        } else {
            pipeline
                .jobs
                .get_mut(&job)
                .unwrap()
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .push(queued);
        }
    }
    let mut work = work.expect("the payload's second article is queued");
    if at_once {
        assert!(refusing);
        work.exclude_servers.insert(0);
        pipeline.book_unservable_work(&work);
        return (temp_dir, pipeline, scenario, job, lost);
    }
    let (retry_count, mut exclude_servers) = (work.retry_count, work.exclude_servers);
    pipeline.hold_test_lane_works(vec![work], false);
    // With no server refusing, server 1 has already said "not found".
    if !refusing {
        exclude_servers.insert(1);
    }
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: job,
            runtime_generation: pipeline.pool_generation,
            segment_id: lost,
            data: Err(crate::pipeline::DownloadError::fetch(
                crate::pipeline::DownloadFailureKind::ArticleNotFound,
                "430 no such article",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: crate::pipeline::DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
    (temp_dir, pipeline, scenario, job, lost)
}

// An article fails once on its server, with a refused login or a broken
// connection, is retried and served. The job later fails on its password:
// the failure the article was retried past is not the job's cause, the
// password is, and the next duplicate is promoted.
#[tokio::test]
async fn a_password_failure_after_an_article_served_past_a_server_failure_promotes_the_duplicate() {
    for kind in [
        crate::pipeline::DownloadFailureKind::Auth,
        crate::pipeline::DownloadFailureKind::EstablishedTransport,
    ] {
        let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(4 * ARTICLE, 3))]);
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
        let spec = scenario.spec();
        let job = admitted_job_id(&pipeline, &spec);
        let duplicate = parked_duplicate(&pipeline, &spec);
        insert_active_job(&mut pipeline, job, spec).await;
        pipeline.active_download_passes.insert(job);
        let work = pipeline
            .jobs
            .get_mut(&job)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        let work = pipeline.dispatched(work);
        let (segment_id, retry_count, exclude_servers) =
            (work.segment_id, work.retry_count, work.exclude_servers);
        pipeline.hold_test_lane_works(vec![work], false);
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: job,
                runtime_generation: pipeline.pool_generation,
                segment_id,
                data: Err(crate::pipeline::DownloadError::fetch(
                    kind,
                    "the server failed",
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: Some(0),
                origin: crate::pipeline::DownloadResultOrigin::NormalPrimary,
                retry_count,
                exclude_servers,
                release_connection_slot: true,
            })
            .await;
        assert!(
            !pipeline.segment_terminal_states.contains_key(&segment_id),
            "{kind:?}: the article is retried, not booked"
        );
        // The retry is served.
        serve_job(
            &mut pipeline,
            job,
            &scenario,
            (segment_id.file_id.file_index, segment_id.segment_number),
        )
        .await;

        let reason = format!(
            "{}the password is wrong",
            crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
        );
        pipeline.jobs.get_mut(&job).unwrap().status = JobStatus::Extracting;
        pipeline.fail_job(job, reason.clone());
        let (error, cause, promotion) = failed_record(&mut pipeline, job).await;
        assert_eq!(error.as_deref(), Some(reason.as_str()), "{kind:?}");
        assert_eq!(
            cause,
            Some(crate::jobs::SemanticTerminalCause::PasswordFailure),
            "{kind:?}"
        );
        assert_eq!(
            promotion,
            crate::jobs::duplicate_persistence::SemanticPromotionState::Pending,
            "{kind:?}"
        );
        let claims = pipeline.db.reconcile_semantic_promotion_claims(8).unwrap();
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
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
    let mut spec = scenario.spec();
    for segment in &mut spec.files[0].segments {
        let first = segment.message_ids.in_use().to_string();
        segment.message_ids =
            crate::jobs::MessageIds::new(vec![first.clone(), format!("again-{first}")]);
    }
    let job = admitted_job_id(&pipeline, &spec);
    let duplicate = parked_duplicate(&pipeline, &spec);
    insert_active_job(&mut pipeline, job, spec).await;
    pipeline.active_download_passes.insert(job);
    refuse_login_past_give_up(&pipeline, 1).await;
    let work = pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = pipeline.dispatched(work);
    let (segment_id, retry_count) = (work.segment_id, work.retry_count);
    // Server 0 has said "not found" under the first id.
    let mut exclude_servers = work.exclude_servers;
    exclude_servers.insert(0);
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: job,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(crate::pipeline::DownloadError::fetch(
                crate::pipeline::DownloadFailureKind::Auth,
                "481 authentication failed",
            )),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(1),
            origin: crate::pipeline::DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
    assert!(
        !pipeline.segment_terminal_states.contains_key(&segment_id),
        "the article moves to its next id, not booked"
    );
    assert!(!pipeline.message_id_left(segment_id));
    serve_job(
        &mut pipeline,
        job,
        &scenario,
        (segment_id.file_id.file_index, segment_id.segment_number),
    )
    .await;

    let reason = format!(
        "{}the password is wrong",
        crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
    );
    pipeline.jobs.get_mut(&job).unwrap().status = JobStatus::Extracting;
    pipeline.fail_job(job, reason.clone());
    let (error, cause, promotion) = failed_record(&mut pipeline, job).await;
    assert_eq!(error.as_deref(), Some(reason.as_str()));
    assert_eq!(
        cause,
        Some(crate::jobs::SemanticTerminalCause::PasswordFailure)
    );
    assert_eq!(
        promotion,
        crate::jobs::duplicate_persistence::SemanticPromotionState::Pending
    );
    let claims = pipeline.db.reconcile_semantic_promotion_claims(8).unwrap();
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
    Option<crate::jobs::SemanticTerminalCause>,
    crate::jobs::duplicate_persistence::SemanticPromotionState,
) {
    let row = history(pipeline, job).await;
    assert_eq!(row.status, "failed");
    let candidate = pipeline
        .db
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
    Option<crate::jobs::SemanticTerminalCause>,
    crate::jobs::duplicate_persistence::SemanticPromotionState,
); 2] {
    let mut records = Vec::new();
    for refusing in [true, false] {
        let (_temp_dir, mut pipeline, _, job, _) =
            with_an_article_lost_by(refusing, at_once && refusing).await;
        assert_eq!(pipeline.login_refusal_words(job).is_some(), refusing);
        pipeline.jobs.get_mut(&job).unwrap().status = stage.clone();
        pipeline.fail_job(job, reason.to_string());
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
            Some(crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth)
        );
        assert_eq!(
            refused.2,
            crate::jobs::duplicate_persistence::SemanticPromotionState::Pending
        );
    }
}

#[tokio::test]
async fn a_wrong_password_past_a_refusing_server_keeps_its_own_cause_and_promotion() {
    let reason = format!(
        "{}the password is wrong",
        crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
    );
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
        pipeline.fail_job_on_health(job, "the post cannot complete".to_string());
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
            Some(crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth),
            crate::jobs::duplicate_persistence::SemanticPromotionState::Pending
        )
    );
    assert_eq!(
        records[1],
        (
            Some("the post cannot complete".to_string()),
            Some(crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth),
            crate::jobs::duplicate_persistence::SemanticPromotionState::Pending
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
    for (at_once, booked) in [
        (
            false,
            crate::pipeline::SegmentTerminalState::UnservableNotFound,
        ),
        (true, crate::pipeline::SegmentTerminalState::Unservable),
    ] {
        let (_temp_dir, mut pipeline, scenario, job, lost) =
            with_an_article_lost_by(true, at_once).await;
        assert_eq!(
            pipeline.segment_terminal_states.get(&lost).copied(),
            Some(booked)
        );
        assert!(pipeline.login_refusal_words(job).is_some());

        drive(&mut pipeline, &scenario, job, |_| false).await;

        let row = history(&mut pipeline, job).await;
        assert_eq!(row.status, "complete");
        assert_eq!(row.error_message, None);
        assert_eq!(failure_kind(&pipeline, job), None);
        assert!(!pipeline.login_refusals.contains_key(&job));
        assert_eq!(
            pipeline
                .db
                .semantic_candidate_snapshot(job)
                .unwrap()
                .and_then(|candidate| candidate.terminal_cause),
            Some(crate::jobs::SemanticTerminalCause::Success)
        );
        // The cause the booked article recorded goes with the job: run again
        // under the same id and failed on its password, the job's cause is
        // its own.
        assert!(!pipeline.semantic_terminal_causes.contains_key(&job));
        insert_active_job(&mut pipeline, job, scenario.spec()).await;
        let reason = format!(
            "{}the password is wrong",
            crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
        );
        pipeline.jobs.get_mut(&job).unwrap().status = JobStatus::Extracting;
        pipeline.fail_job(job, reason.clone());
        let (error, cause, _) = failed_record(&mut pipeline, job).await;
        assert_eq!(error.as_deref(), Some(reason.as_str()));
        assert_eq!(
            cause,
            Some(crate::jobs::SemanticTerminalCause::PasswordFailure)
        );
    }
}

// A queue script subscribed to NZB_DOWNLOADED: the job's last decode brings
// it to the barrier, where it waits for the script instead of moving.
#[tokio::test]
async fn the_last_decode_holds_the_job_at_the_nzb_downloaded_barrier() {
    let scenario = Scenario::new(vec![Post::plain("payload.bin", payload(3 * ARTICLE, 2))]);
    let (_temp_dir, mut pipeline, _) = admit_with(&scenario, |pipeline, root| {
        enable_queue_script(&pipeline.db, root);
    })
    .await;
    while let Some(work) = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
    {
        let article = (
            work.segment_id.file_id.file_index,
            work.segment_id.segment_number,
        );
        serve_job(&mut pipeline, JOB, &scenario, article).await;
        // The tail of every decode: the last one ends the download pass.
        if pipeline.job_decode_stage_drained(JOB) {
            pipeline.maybe_finish_download_pass(JOB);
        }
    }
    loop {
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pipeline.pump_decode_queue();
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        break;
    }

    assert!(
        pipeline.queue_script_waiters.contains(&JOB),
        "the barrier is not held; status {:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    assert!(pipeline.inflight_moves.is_empty());
    let TerminalPostProcessingEvent::QueueAdmitted(id) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue admission");
    };
    assert_eq!(id, JOB);
    pipeline.handle_queue_scripts_admitted(id);
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::AwaitingQueueScripts)
    );
}

// The job downloaded and settled, its final move started and not yet
// heard back: the moment just before it delivers.
async fn at_the_move(scenario: &Scenario) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    let (temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    drive(&mut pipeline, scenario, JOB, |pipeline| {
        pipeline.inflight_moves.contains(&JOB)
    })
    .await;
    assert!(
        pipeline.inflight_moves.contains(&JOB),
        "the move did not start; status {:?}",
        job_status_for_assert(&pipeline, JOB)
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
    let mut events = pipeline.event_tx.subscribe();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

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
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_none());

    drop(release);
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;
    assert!(
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .all(|info| info.job_id != JOB || is_terminal_status(&info.status)),
        "the job is published as finished before its terminal event"
    );
    // No flush: the row is readable as soon as the event is heard.
    assert_eq!(
        pipeline
            .db
            .get_job_history(JOB.0)
            .unwrap()
            .map(|row| row.status),
        Some("complete".to_string())
    );
    let listed = pipeline
        .db
        .list_job_history(&crate::HistoryFilter::default())
        .unwrap();
    assert!(listed.iter().any(|row| row.job_id == JOB.0));
}

// A job retiring into history refuses every mutation until its archive is
// acknowledged, and none of them moves the queue's revision.
#[tokio::test]
async fn a_retiring_job_refuses_mutations_until_its_history_commits() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)
    }));
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
                crate::JobUpdate {
                    category: crate::jobs::FieldUpdate::Set("changed-category".into()),
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
    let handle = handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

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
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_some());
}

// What a polling client reads of the job, by id and in the job list, and
// whether that shows it finished.
fn shown_finished(handle: &SchedulerHandle) -> bool {
    let by_id = handle
        .get_job(JOB)
        .is_ok_and(|job| is_terminal_status(&job.status));
    let listed = handle
        .list_jobs()
        .iter()
        .any(|job| job.job_id == JOB && is_terminal_status(&job.status));
    by_id || listed
}

// A job shown finished is one history can remove: its history row is
// written and the queue no longer holds it, so neither history delete
// answers that the job is still active.
fn removable(pipeline: &Pipeline, handle: &SchedulerHandle) -> bool {
    !handle.queued_job_ids().contains(&JOB) && pipeline.db.get_job_history(JOB.0).unwrap().is_some()
}

// Every read a polling client makes through the terminal sequence: while the
// history archive is held the job reads as being finished, never finished,
// and history refuses it; once the archive commits it reads finished, and
// NZBGet's HistoryDelete and HistoryFinalDelete both take it.
#[tokio::test]
async fn a_job_shown_finished_can_be_removed_from_history() {
    let scenario = delivered();
    let (_temp_dir, mut pipeline, _) = at_the_move(&scenario).await;
    let handle = handle(&pipeline);
    let mut events = handle.subscribe_events();
    let release = hold_database_writer(&pipeline);
    settle_inflight_moves(&mut pipeline).await;

    // Retiring, the archive queued and not committed.
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)
    }));
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
    pipeline.db.flush_write_queue().await.unwrap();
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
    assert_eq!(pipeline.db.archive_nzbget_history(&[JOB.0]).unwrap(), 1);
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

    match job_status_for_assert(&pipeline, JOB) {
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

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Partial)
    );
    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    assert_eq!(snapshot.files_missing_total, 1);
    assert_eq!(snapshot.missing_segments_total, 2);
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

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Partial)
    );
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
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Complete | JobStatus::Partial)
        ),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
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
        pipeline
            .list_jobs()
            .into_iter()
            .find(|job| job.job_id == JOB)
            .expect("the job is listed")
            .file_remaining_bytes
    };
    assert_eq!(
        snapshot(&pipeline),
        vec![alpha.iter().sum::<u64>(), beta.iter().sum::<u64>()]
    );

    serve_job(&mut pipeline, JOB, &scenario, (0, 0)).await;
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
    let handle = handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;

    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.deferred_moves.contains(&JOB)
    })
    .await;
    assert!(
        pipeline.deferred_moves.contains(&JOB),
        "the move was not held; status {:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|job| job.job_id == JOB)
        .expect("the held job is still listed");
    assert_eq!(info.total_files, 2);
    assert_eq!(info.completed_files, info.total_files);
    assert!(!ended(&pipeline, JOB));

    handle.resume_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
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
    let mut events = pipeline.event_tx.subscribe();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    wait_for_job_event(
        &mut events,
        |event| matches!(event, PipelineEvent::JobCompleted { job_id } if *job_id == JOB),
    )
    .await;

    let finished = pipeline
        .shared_state
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
    let handle = handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    // Every article decoded into the graph while the pause is written and
    // not yet applied: the stream's run is asked for in that window.
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|graph| graph.remaining_bytes().iter().all(|left| *left == 0))
    })
    .await;
    handle.resume_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;

    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
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
