// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::download::scheduler::Handout;

// A client over `levels`: each server's priority group and backfill flag.
// Nothing dials; the scheduler only reads the policy.
fn ladder_client(levels: &[(u32, bool)]) -> NntpClient {
    ladder_client_with_quota(levels, None)
}

// As [`ladder_client`], with one server metered by `quota`'s control.
pub(in crate::pipeline::tests) fn ladder_client_with_quota(
    levels: &[(u32, bool)],
    quota: Option<(usize, Arc<weaver_nntp::transfer::ServerTransferControl>)>,
) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: levels
            .iter()
            .enumerate()
            .map(|(idx, &(group, backfill))| {
                let mut config = weaver_nntp::pool::ServerPoolConfig {
                    max_connections: 100,
                    group,
                    backfill,
                    stable_id: weaver_nntp::transfer::StableServerId(idx as u32),
                    transfer_control: quota
                        .as_ref()
                        .filter(|(metered, _)| *metered == idx)
                        .map(|(_, control)| Arc::clone(control)),
                    ..Default::default()
                };
                // Nothing dials; the name is what a failure's words say.
                config.server.host = format!("s{idx}.example");
                config
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    })
}

pub(in crate::pipeline::tests) async fn ladder_pipeline(
    temp_dir: &TempDir,
    levels: &[(u32, bool)],
) -> Pipeline {
    let (mut pipeline, _, _) = crate::pipeline::tests::new_scheduler_pipeline(temp_dir).await;
    pipeline.nntp = Arc::new(ladder_client(levels));
    pipeline
}

// Answer `work`, handed to `server`, "not found" through the result path a
// lane's 430 takes, so the exclusion, the requeue and any booking are the
// product's own.
pub(in crate::pipeline::tests) async fn answer_not_found(
    pipeline: &mut Pipeline,
    server: usize,
    work: DownloadWork,
) {
    answer_failed(
        pipeline,
        server,
        work,
        DownloadFailureKind::ArticleNotFound,
        "430 no such article",
    )
    .await;
}

// Fail the connection under `work`, handed to `server`, through the result
// path a lane's lost connection takes: an ordinary retry, which refuses the
// article to no server.
#[cfg(test)]
pub(in crate::pipeline::tests) async fn answer_connection_failed(
    pipeline: &mut Pipeline,
    server: usize,
    work: DownloadWork,
) {
    answer_failed(
        pipeline,
        server,
        work,
        DownloadFailureKind::EstablishedTransport,
        "connection closed",
    )
    .await;
}

async fn answer_failed(
    pipeline: &mut Pipeline,
    server: usize,
    work: DownloadWork,
    kind: DownloadFailureKind,
    said: &str,
) {
    let (job_id, segment_id) = (work.segment_id.file_id.job_id, work.segment_id);
    let (retry_count, exclude_servers) = (work.retry_count, work.exclude_servers);
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(DownloadError::fetch(kind, said)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(server),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
}

pub(in crate::pipeline::tests) fn ask(
    pipeline: &mut Pipeline,
    server_idx: usize,
    want: usize,
) -> Vec<DownloadWork> {
    let pressure = pipeline.refresh_download_pressure();
    match pipeline.next_works(server_idx, want, None, pressure) {
        Handout::Works(works) => works,
        Handout::Idle => Vec::new(),
        Handout::Yield(reason) => panic!("a whole-link gate fired: {reason:?}"),
        Handout::Saturated(wake) => panic!("no lane share was booked: {wake:?}"),
    }
}

pub(in crate::pipeline::tests) fn rewrite_queue(
    pipeline: &mut Pipeline,
    job_id: JobId,
    mut edit: impl FnMut(&mut DownloadWork),
) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let queue = &mut state.articles.active_mut().unwrap().download_queue;
    let mut works: Vec<DownloadWork> = queue
        .drain_all()
        .into_iter()
        .map(|item| item.dispatched(&state.spec))
        .collect();
    for work in &mut works {
        edit(work);
    }
    let queue = &mut state.articles.active_mut().unwrap().download_queue;
    for work in works {
        queue.push(work);
    }
}

impl DispatchFixture {
    pub async fn ladder(temp: &TempDir, levels: &[(u32, bool)]) -> Self {
        Self(ladder_pipeline(temp, levels).await)
    }

    pub async fn refuse_login(&self, server: usize) {
        refuse_login_past_give_up(&self.0, server).await;
    }

    pub async fn start_pass(&mut self, id: JobId, spec: JobSpec) {
        insert_active_job(&mut self.0, id, spec).await;
        self.0.active_download_passes.insert(id);
    }

    pub fn ask(&mut self, server: usize, want: usize) -> Vec<DownloadWork> {
        ask(&mut self.0, server, want)
    }

    pub async fn answer_not_found(&mut self, server: usize, work: DownloadWork) {
        answer_not_found(&mut self.0, server, work).await;
    }

    pub fn rewrite_queue(&mut self, id: JobId, edit: impl FnMut(&mut DownloadWork)) {
        rewrite_queue(&mut self.0, id, edit);
    }

    pub fn lost_articles(&self) -> u64 {
        self.0.metrics.articles_not_found.load(Ordering::Relaxed)
    }

    pub fn dead_post_reason(&self, id: JobId) -> Option<&String> {
        self.0.post_probes.dead.get(&id)
    }

    pub fn missing_and_unservable_counts(&self) -> (usize, usize) {
        let booked = |state| {
            self.0
                .segment_terminal_states
                .values()
                .filter(|held| **held == state)
                .count()
        };
        (
            booked(SegmentTerminalState::Missing),
            booked(SegmentTerminalState::UnservableNotFound),
        )
    }

    pub fn terminal_cause(&self, id: JobId) -> Option<crate::jobs::SemanticTerminalCause> {
        self.0.semantic_terminal_causes.get(&id).copied()
    }
}
