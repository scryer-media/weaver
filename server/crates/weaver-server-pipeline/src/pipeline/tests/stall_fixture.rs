// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::dispatch_fixture::audited_pipeline_with_backlog as new_direct_pipeline_with_backlog;
use super::*;
use crate::pipeline::download;
use weaver_server_disk::operations::disk::volume::Volume;

pub const DOWNLOAD_DISPATCH_STALL_KICK_AFTER: Duration =
    download::DOWNLOAD_DISPATCH_STALL_KICK_AFTER;
pub const DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER: Duration =
    download::DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER;
pub const DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL: Duration =
    download::DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL;

pub fn parse(value: Option<String>) -> bool {
    download::download_stall_recovery_from_env_value(value)
}

pub fn job(pipeline: &Pipeline, id: JobId) -> &JobState {
    &pipeline.jobs[&id]
}

pub fn job_mut(pipeline: &mut Pipeline, id: JobId) -> &mut JobState {
    pipeline.jobs.get_mut(&id).unwrap()
}

pub fn client(pipeline: &Pipeline) -> &NntpClient {
    &pipeline.nntp
}

pub fn set_client(pipeline: &mut Pipeline, client: NntpClient) {
    pipeline.nntp = Arc::new(client);
}

pub fn activate_client(pipeline: &mut Pipeline, client: NntpClient, total: usize, generation: u64) {
    pipeline.activate_nntp(client, total, generation);
}

pub fn set_recovery(pipeline: &mut Pipeline, enabled: bool) {
    pipeline.download_stall_recovery = enabled;
}

pub fn recovery(pipeline: &Pipeline) -> bool {
    pipeline.download_stall_recovery
}

pub fn propagation(
    pipeline: &mut Pipeline,
) -> &mut std::collections::HashMap<JobId, (Instant, i64)> {
    &mut pipeline.propagation_ready_at
}

pub fn durable(pipeline: &mut Pipeline) -> &mut std::collections::HashMap<JobId, Instant> {
    &mut pipeline.download_restart_durable_lead_retry_after
}

pub fn set_delay(pipeline: &mut Pipeline, delay: Duration) {
    pipeline.propagation_delay = delay;
}

pub fn earliest(pipeline: &Pipeline, now: Instant) -> Option<Instant> {
    pipeline.earliest_explained_dispatch_wait(now)
}

pub fn dispatch_at(pipeline: &mut Pipeline, now: Instant) {
    pipeline.dispatch_downloads_at(now);
}

pub fn stall_since(pipeline: &Pipeline) -> Option<Instant> {
    pipeline
        .download_dispatch_stall
        .as_ref()
        .map(|watch| watch.since)
}

pub fn kick(pipeline: &Pipeline) -> Option<&(Instant, std::collections::HashMap<JobId, u64>)> {
    pipeline
        .download_dispatch_stall
        .as_ref()
        .and_then(|watch| watch.kick.as_ref())
}

pub fn watch_from(pipeline: &mut Pipeline, since: Instant) {
    pipeline.download_dispatch_stall = Some(download::DispatchStallWatch {
        since,
        segments_downloaded: pipeline.metrics.segments_downloaded.load(Ordering::Relaxed),
        kick: None,
    });
}

pub fn last_log(pipeline: &Pipeline) -> Option<Instant> {
    pipeline.last_download_dispatch_stall_log_at
}

pub fn liveness_since(pipeline: &Pipeline) -> Option<Instant> {
    pipeline.download_dispatch_liveness_since
}

pub fn infra_count(pipeline: &Pipeline) -> usize {
    pipeline.infrastructure_retries.len()
}

pub fn working_volume(pipeline: &Pipeline, id: JobId) -> Arc<Volume> {
    Arc::clone(
        pipeline.graph_jobs[&id]
            .disk()
            .expect("the job writes through its volume's writer")
            .working()
            .volume(),
    )
}

pub async fn activate_queued(pipeline: &mut Pipeline, id: JobId, n: usize) {
    dispatch_fixture::scheduler_activation::add_queued_job(pipeline, id, n).await;
    use crate::jobs::persistence::JobsPersistenceInternalQueries;
    pipeline.db.set_nzb_admitted(id, true).unwrap();
    pipeline.nzb_admitted.insert(id);
    pipeline.request_job_activation(id);
}

pub fn eligible(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.download_scheduler_eligible_jobs()
}

pub fn hold_until(pipeline: &mut Pipeline, id: JobId) -> Option<Instant> {
    pipeline.propagation_hold_until(id)
}

pub fn tell_verdicts(pipeline: &mut Pipeline) {
    pipeline.tell_login_refusal_verdicts();
}

pub fn report_holds(pipeline: &mut Pipeline) {
    pipeline.report_changed_dispatch_holds();
}

pub fn dispatch_wake(pipeline: &Pipeline) -> bool {
    pipeline.download_dispatch_wake
}

pub fn connections(pipeline: &Pipeline) -> usize {
    pipeline.active_download_connections
}

pub fn metrics_arc(pipeline: &Pipeline) -> Arc<PipelineMetrics> {
    Arc::clone(&pipeline.metrics)
}
#[derive(Clone, Copy)]
pub struct LoginView(download::LoginRefusalView);
impl LoginView {
    pub fn waiting(self) -> weaver_nntp::ServerSet {
        self.0.waiting
    }
}

pub fn login_view(pipeline: &Pipeline) -> LoginView {
    LoginView(pipeline.login_refusal_view())
}

pub fn login_wait(
    pipeline: &Pipeline,
    job: JobId,
    view: LoginView,
) -> Option<(weaver_nntp::ServerSet, bool)> {
    pipeline
        .job_login_refusal_wait(job, view.0, None)
        .map(|wait| (wait.servers, wait.every))
}

pub fn wait_reason(
    pipeline: &Pipeline,
    job: JobId,
    view: LoginView,
) -> Option<std::borrow::Cow<'static, str>> {
    pipeline.dispatch_wait_reason(job, view.0)
}

pub fn released_work(
    pipeline: &mut Pipeline,
    job: JobId,
    view: LoginView,
) -> Vec<download::QueueItem> {
    pipeline.take_stall_released_work(job, view.0)
}

pub async fn audited(temp: &TempDir) -> Pipeline {
    new_direct_pipeline_with_backlog(temp, 6 * 1024 * 1024, 1)
        .await
        .0
}
// A pipeline with no connections: every pass with queued work ends with
// none open, which is the stall's shape. Its pool still has one server to
// rank, so the stall is not the outage the watchdog leaves alone.
pub async fn stalled_pipeline(temp_dir: &tempfile::TempDir) -> Pipeline {
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

pub async fn insert_stalled_job(pipeline: &mut Pipeline, job_id: JobId, segments: usize) {
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
pub fn park_indefinite_infrastructure_retry(pipeline: &mut Pipeline, job_id: JobId) {
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
    let work = work.dispatched(&pipeline.jobs[&job_id].spec);
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

pub fn kicks(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_dispatch_stall_kicks
        .load(Ordering::Relaxed)
}

pub fn releases(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .download_dispatch_stall_releases
        .load(Ordering::Relaxed)
}

// Regression fixtures use real dispatch gates and nonzero capacity. Aging
// the watch tests the release decision, not elapsed provider retry history.
pub async fn verification_pipeline(temp: &tempfile::TempDir, two_servers: bool) -> Pipeline {
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

pub fn verification_run_real_kick(pipeline: &mut Pipeline) {
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

pub fn verification_old_kick(pipeline: &mut Pipeline, jobs: &[JobId]) {
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

pub async fn verification_exclude_healthy_and_cool_remaining(
    pipeline: &mut Pipeline,
    job_id: JobId,
) {
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

// The wait reason naming pool servers `servers` as refusing the login.
pub fn login_refused_name(pipeline: &Pipeline, servers: &[usize]) -> String {
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

// The wait reason the job's queue row stores.
pub fn stored_reason(pipeline: &Pipeline, job: JobId) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(job)
            .and_then(|row| row.report.job.download_wait_reason.clone())
    })
}

// A stalled job whose three articles server 0 lacks, on the two-server
// pipeline, with server 1 just begun refusing the login and the row told.
pub async fn job_waiting_on_server_1(temp: &tempfile::TempDir, job: JobId) -> Pipeline {
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

// Two jobs, each with all its work waiting on server 1, which refuses the
// login.
pub async fn two_jobs_waiting_on_server_1(
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
pub fn queued_for_download(pipeline: &Pipeline, job: JobId) -> usize {
    pipeline
        .jobs
        .get(&job)
        .and_then(|state| state.articles.active())
        .map_or(0, |active| active.download_queue.len())
}
