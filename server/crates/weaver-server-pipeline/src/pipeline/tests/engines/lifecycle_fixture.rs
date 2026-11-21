// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::duplicate_persistence::JobsDuplicatePersistenceAdmitDuplicateSubmissionQueries;
use std::collections::VecDeque;
use std::sync::{Arc, atomic::Ordering};

use super::actor_drive;
use super::harness::{ARTICLE, Post, par2_set, payload};
use super::harness::{Scenario, admit_with, serve_job};
use crate::pipeline::TerminalPostProcessingEvent;
use crate::pipeline::tests::{
    history_fixture::database, job_status_for_assert, script_fixture::enable_queue_script,
};
use crate::pipeline::tests::{
    insert_active_job, new_direct_pipeline, refuse_login_past_give_up, retention_client,
    two_server_retention_client,
};
use crate::pipeline::{
    DownloadBatchLease, DownloadLaneMode, DownloadResult, OwnedDownloadLaneEvent,
    SegmentTerminalState,
};
use crate::{DownloadWork, SegmentId};
use crate::{JobId, JobInfo, JobStatus, Pipeline};

pub fn job_status(pipeline: &Pipeline, id: JobId) -> Option<JobStatus> {
    job_status_for_assert(pipeline, id)
}

pub async fn drive(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    job: JobId,
    until: impl Fn(&Pipeline) -> bool,
) {
    actor_drive::drive(pipeline, scenario, job, until).await;
}

pub fn ended(pipeline: &Pipeline, job: JobId) -> bool {
    actor_drive::ended(pipeline, job)
}

pub async fn admit_with_queue_scripts(
    scenario: &Scenario,
) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    admit_with(scenario, |pipeline, root| {
        enable_queue_script(database(pipeline), root)
    })
    .await
}

pub async fn serve_one(
    pipeline: &mut Pipeline,
    id: JobId,
    scenario: &Scenario,
    article: (u32, u32),
) {
    serve_job(pipeline, id, scenario, article).await;
}

pub async fn drive_to_downloaded_barrier(pipeline: &mut Pipeline, scenario: &Scenario, job: JobId) {
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
        // The tail of every decode: the last one ends the download pass.
        if pipeline.job_decode_stage_drained(job) {
            pipeline.maybe_finish_download_pass(job);
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
            .get(&job)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        break;
    }
}

pub async fn receive_queue_admission(pipeline: &mut Pipeline) -> Option<JobId> {
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::QueueAdmitted(id) => Some(id),
        _ => None,
    }
}

pub fn live_jobs(pipeline: &Pipeline) -> Vec<JobInfo> {
    pipeline.list_jobs()
}

pub fn missing_totals(pipeline: &Pipeline) -> (u64, u64) {
    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    (
        snapshot.files_missing_total,
        snapshot.missing_segments_total,
    )
}

pub fn graph_bytes_drained(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline
        .graph_jobs
        .get(&job)
        .is_some_and(|graph| graph.remaining_bytes().iter().all(|left| *left == 0))
}

pub fn queued_len(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline.jobs[&job]
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

pub async fn refused_probe_for_first_article(
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
    let work = work.dispatched(&pipeline.jobs[&job].spec);
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

pub fn login_refused_reason(pipeline: &Pipeline, server: usize) -> String {
    let identity = &pipeline.nntp.pool().server_identities()[server];
    format!(
        "{}:{}:{}",
        crate::jobs::handle::LOGIN_REFUSED_WAIT_REASON,
        identity.host,
        identity.port
    )
}

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

pub async fn run_seated_lanes(pipeline: &mut Pipeline) {
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

pub async fn two_jobs_on_one_server(pipeline: &mut Pipeline, first: JobId, second: JobId) {
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    for (job, name, seed) in [(first, "first.bin", 11), (second, "second.bin", 12)] {
        let mut spec = Scenario::new(vec![Post::plain(name, payload(4 * ARTICLE, seed))]).spec();
        spec.name = format!("Waiting {}", job.0);
        insert_active_job(pipeline, job, spec).await;
        pipeline.active_download_passes.insert(job);
    }
}

pub fn queued_or_none(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline
        .jobs
        .get(&job)
        .and_then(|state| state.articles.active())
        .map_or(0, |active| active.download_queue.len())
}

pub async fn with_an_article_lost(
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

pub async fn with_an_article_lost_by(
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
        file_id: weaver_server_base::jobs::ids::NzbFileId {
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
    for queued in queued {
        let queued = queued.dispatched(&pipeline.jobs[&job].spec);
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

pub async fn try_refused_first_lane(pipeline: &mut Pipeline, job: JobId) -> PendingLaneResults {
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
    let work = work.dispatched(&pipeline.jobs[&job].spec);
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
    PendingLaneResults(pending)
}

pub struct PendingLaneResults(VecDeque<DownloadResult>);

impl PendingLaneResults {
    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn source(&self) -> Option<usize> {
        self.0[0].source_server_idx
    }

    pub async fn deliver(self, pipeline: &mut Pipeline) {
        for result in self.0 {
            pipeline.process_released_download_done(result).await;
        }
    }
}
pub struct RefusingServer(super::harness::Listener);

impl RefusingServer {
    pub fn bind(pipeline: &mut Pipeline) -> Self {
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
        Self(server)
    }

    pub fn port(&self) -> u16 {
        self.0.port
    }
}
pub fn admitted_job_id(pipeline: &Pipeline, spec: &crate::jobs::model::JobSpec) -> JobId {
    let request = crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(spec, [60; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source(&spec.name, 10),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: crate::pipeline::tests::sample_nzb_zstd(),
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

pub fn client(pipeline: &Pipeline) -> &weaver_nntp::client::NntpClient {
    &pipeline.nntp
}

pub fn use_retention_servers(pipeline: &mut Pipeline, days: &[u32]) {
    pipeline.nntp = Arc::new(retention_client(days));
}

pub fn use_two_retention_servers(pipeline: &mut Pipeline) {
    pipeline.nntp = Arc::new(two_server_retention_client());
}

pub async fn refuse_past_give_up(pipeline: &Pipeline, server: usize) {
    refuse_login_past_give_up(pipeline, server).await;
}

pub fn age_and_recheck_excludes(
    pipeline: &mut Pipeline,
    id: JobId,
    days: u64,
) -> weaver_nntp::ServerSet {
    crate::pipeline::tests::age_job_days(pipeline, id, days);
    pipeline.clear_job_retention_excludes(id);
    pipeline.job_retention_excludes(id)
}

pub fn mark_download_pass(pipeline: &mut Pipeline, job: JobId) {
    pipeline.active_download_passes.insert(job);
}

pub fn not_found(pipeline: &Pipeline) -> u64 {
    pipeline.metrics.articles_not_found.load(Ordering::Relaxed)
}

pub fn terminal_count(pipeline: &Pipeline) -> usize {
    pipeline.segment_terminal_states.len()
}

pub fn booked_terminal(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.segment_terminal_states.contains_key(&id)
}

pub fn unservable_count(pipeline: &Pipeline) -> usize {
    pipeline
        .segment_terminal_states
        .values()
        .filter(|state| **state == SegmentTerminalState::Unservable)
        .count()
}

pub fn pending_retry_count(pipeline: &Pipeline, id: JobId) -> Option<usize> {
    pipeline.pending_retries_by_job.get(&id).copied()
}

pub fn infrastructure_waits(pipeline: &Pipeline) -> usize {
    pipeline.infrastructure_retries.len()
}

pub fn failure_stage(
    pipeline: &Pipeline,
    id: JobId,
) -> Option<crate::post_processing::model::PipelineFailureStage> {
    pipeline.pipeline_failure_stages.get(&id).copied()
}

pub fn dispatch_wait(pipeline: &Pipeline, id: JobId) -> Option<std::borrow::Cow<'static, str>> {
    pipeline.dispatch_wait_reason(id, pipeline.login_refusal_view())
}

pub fn retire_unservable(pipeline: &mut Pipeline, id: JobId) -> usize {
    pipeline.retire_unservable_queued_work(id)
}

pub fn no_server_can_serve(
    pipeline: &mut Pipeline,
    id: JobId,
    excludes: weaver_nntp::ServerSet,
) -> bool {
    pipeline.no_server_can_serve(id, excludes)
}

pub fn wake_infrastructure(pipeline: &mut Pipeline) -> usize {
    pipeline.wake_all_infrastructure_retries()
}

pub fn has_download_wait(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.download_wait_by_job.contains_key(&id)
}

pub fn hold_for_disk(pipeline: &mut Pipeline, id: JobId, held: bool) {
    if held {
        pipeline.disk_waiting.insert(id);
    } else {
        pipeline.disk_waiting.remove(&id);
    }
}

pub fn dispatch(pipeline: &mut Pipeline) {
    pipeline.dispatch_downloads();
}

pub fn connections(pipeline: &Pipeline) -> usize {
    pipeline.active_download_connections
}

pub fn has_connections(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .active_download_connections_by_job
        .contains_key(&id)
}

pub fn tell_refusal_verdicts(pipeline: &mut Pipeline) {
    pipeline.tell_login_refusal_verdicts();
}

pub fn set_pause_intent(pipeline: &Pipeline, id: JobId, paused: bool) {
    pipeline
        .shared_state
        .queue
        .set_job_paused(id, paused)
        .unwrap();
}

pub fn queued_excludes(pipeline: &Pipeline, id: SegmentId) -> Option<weaver_nntp::ServerSet> {
    pipeline.jobs[&id.file_id.job_id]
        .articles
        .active()
        .unwrap()
        .download_queue
        .peek_first_matching(|work| work.segment_id == id)
        .map(|work| work.exclude_servers)
}

pub fn has_message_id_left(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.message_id_left(id)
}

pub fn booked_as_unservable(pipeline: &Pipeline, id: SegmentId, not_found: bool) -> bool {
    let expected = if not_found {
        SegmentTerminalState::UnservableNotFound
    } else {
        SegmentTerminalState::Unservable
    };
    pipeline.segment_terminal_states.get(&id).copied() == Some(expected)
}

pub const PASSWORD_MARKER: &str = crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER;

pub struct HeldAuthWork(Vec<crate::pipeline::download::queue::QueueItem>);

impl HeldAuthWork {
    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn park_first(
        &mut self,
        pipeline: &mut Pipeline,
        id: JobId,
        excluded: weaver_nntp::ServerSet,
    ) {
        self.0[0].exclude_servers = excluded;
        pipeline.download_wait_by_job.insert(
            id,
            crate::pipeline::DownloadWaitStatus {
                reason: crate::pipeline::AUTH_WAIT_REASON,
                retry_at_epoch_ms: None,
                pending_count: 1,
                exclude_servers: excluded,
            },
        );
    }

    pub fn requeue(self, pipeline: &mut Pipeline, id: JobId) {
        pipeline.download_wait_by_job.remove(&id);
        let queue = &mut pipeline
            .jobs
            .get_mut(&id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue;
        for work in self.0 {
            queue.push(work);
        }
    }
}

pub fn take_auth_work(pipeline: &mut Pipeline, id: JobId) -> HeldAuthWork {
    HeldAuthWork(
        pipeline
            .jobs
            .get_mut(&id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .drain_all(),
    )
}

#[derive(Debug, Clone, Copy)]
pub enum FetchFailure {
    Auth,
    EstablishedTransport,
    ArticleNotFound,
}

pub async fn fail_first_article(
    pipeline: &mut Pipeline,
    id: JobId,
    failure: FetchFailure,
    message: &'static str,
    source: usize,
    exclude: Option<usize>,
) -> SegmentId {
    let work = pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work: DownloadWork = work.dispatched(&pipeline.jobs[&id].spec);
    finish_failed_article(pipeline, work, failure, message, source, exclude).await
}

pub async fn first_article_not_found(pipeline: &mut Pipeline, id: JobId) -> SegmentId {
    let queue = &mut pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue;
    let mut queued = queue.drain_all();
    let lost = queued.remove(0);
    for remaining in queued {
        queue.push(remaining);
    }
    let lost = lost.dispatched(&pipeline.jobs[&id].spec);
    finish_failed_article(
        pipeline,
        lost,
        FetchFailure::ArticleNotFound,
        "430 no such article",
        0,
        None,
    )
    .await
}

async fn finish_failed_article(
    pipeline: &mut Pipeline,
    work: DownloadWork,
    failure: FetchFailure,
    message: &'static str,
    source: usize,
    exclude: Option<usize>,
) -> SegmentId {
    let (segment_id, retry_count, mut exclude_servers) =
        (work.segment_id, work.retry_count, work.exclude_servers);
    let id = segment_id.file_id.job_id;
    if let Some(server) = exclude {
        exclude_servers.insert(server);
    }
    pipeline.hold_test_lane_works(vec![work], false);
    use crate::pipeline::DownloadFailureKind;
    let kind = match failure {
        FetchFailure::Auth => DownloadFailureKind::Auth,
        FetchFailure::EstablishedTransport => DownloadFailureKind::EstablishedTransport,
        FetchFailure::ArticleNotFound => DownloadFailureKind::ArticleNotFound,
    };
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: id,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(crate::pipeline::DownloadError::fetch(kind, message)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(source),
            origin: crate::pipeline::DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
    segment_id
}
