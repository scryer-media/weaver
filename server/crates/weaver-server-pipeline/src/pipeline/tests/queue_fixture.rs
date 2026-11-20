// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::handle::CancellationOrigin;

pub fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

pub async fn add(handle: &SchedulerHandle, id: JobId) {
    let spec = segmented_job_spec("queue fixture", "payload.bin", &[100]);
    let nzb_zstd = nzb_zstd_for(&spec);
    handle
        .add_job(id, spec, PathBuf::from("queue-fixture.nzb"), nzb_zstd)
        .await
        .unwrap();
}

pub async fn insert_queued_jobs(pipeline: &mut Pipeline, ids: &[JobId]) {
    for job_id in ids {
        insert_active_job(
            pipeline,
            *job_id,
            standalone_job_spec(
                &format!("Batch {}", job_id.0),
                &[(format!("batch-{}.bin", job_id.0), 64)],
            ),
        )
        .await;
    }
    pipeline.report_all_for_test();
}

pub async fn cancel_batch(
    pipeline: &mut Pipeline,
    ids: &[JobId],
) -> crate::jobs::CancelJobsOutcome {
    pipeline.report_all_for_test();
    let mut events = pipeline.event_tx.subscribe();
    let outcome = pipeline
        .shared_state
        .queue
        .cancel_jobs(ids.iter().copied().collect(), CancellationOrigin::User);
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(pipeline).await;
    // Every accepted id whose archive committed announces itself; one whose
    // archive failed is live again instead.
    let mut pending: BTreeSet<_> = outcome
        .iter()
        .filter_map(|(id, result)| result.is_ok().then_some(*id))
        .filter(|id| !pipeline.jobs.contains_key(id))
        .collect();
    while !pending.is_empty() {
        if let PipelineEvent::JobCancelled { job_id } = events.recv().await.unwrap() {
            pending.remove(&job_id);
        }
    }
    outcome
}

pub fn installed_spec(pipeline: &Pipeline, id: JobId) -> Option<&JobSpec> {
    pipeline.jobs.get(&id).map(|job| &job.spec)
}

pub async fn apply_intents(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
}

pub fn report_all(pipeline: &Pipeline) {
    pipeline.report_all_for_test();
}

pub fn set_moving(pipeline: &mut Pipeline, id: JobId) {
    pipeline.inflight_moves.insert(id);
}

pub fn move_to_status(
    pipeline: &mut Pipeline,
    id: JobId,
    status: JobStatus,
) -> weaver_server_base::jobs::model::PostState {
    let state = pipeline.jobs.get_mut(&id).unwrap();
    let lane = state.post_state;
    state.status = status;
    state.refresh_runtime_lanes_from_status();
    let entered = state.post_state;
    pipeline.metrics.post_state_moved(lane, entered);
    entered
}

pub fn metrics(pipeline: &Pipeline) -> &PipelineMetrics {
    &pipeline.metrics
}

pub fn set_retention_servers(pipeline: &mut Pipeline) {
    pipeline.nntp = std::sync::Arc::new(two_server_retention_client());
}

pub fn note_missing_after_refusal(pipeline: &mut Pipeline, job_id: JobId, server: usize) {
    pipeline.note_login_refusals(job_id, [server].into_iter().collect());
    pipeline.semantic_terminal_causes.insert(
        job_id,
        crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth,
    );
}

pub fn login_refusal_words(pipeline: &Pipeline, id: JobId) -> Option<String> {
    pipeline.login_refusal_words(id)
}
pub fn has_login_refusals(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.login_refusals.contains_key(&id)
}
pub fn has_terminal_cause(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.semantic_terminal_causes.contains_key(&id)
}

pub fn fail_on_health(pipeline: &mut Pipeline, id: JobId, error: String) {
    pipeline.fail_job_on_health(id, error);
}
pub fn installed_status(pipeline: &Pipeline, id: JobId) -> Option<&JobStatus> {
    pipeline.jobs.get(&id).map(|state| &state.status)
}

pub struct CancelCheckpoint {
    pending: crate::pipeline::graph::checkpoint::Pending,
    control: crate::persistence::checkpoint_write::CheckpointControl,
}

impl CancelCheckpoint {
    pub fn observe(pipeline: &Pipeline, id: JobId) -> Self {
        let pending = pipeline.graph_jobs[&id].pending().clone();
        let control = pending.control();
        Self { pending, control }
    }
    pub fn stamp(&self) -> u64 {
        self.pending.stamp()
    }
    pub fn revision(&self) -> u64 {
        self.control.revision()
    }
    pub fn cancelled(&self) -> bool {
        self.control.cancelled()
    }
}

pub fn checkpoint_node_count(pipeline: &Pipeline, id: JobId) -> usize {
    use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
    pipeline
        .db
        .load_graph_checkpoint(id)
        .unwrap()
        .unwrap()
        .nodes
        .len()
}

pub fn cancel_and_retire_flags(pipeline: &Pipeline, id: JobId) -> Option<(bool, bool, bool)> {
    pipeline.shared_state.queue.read(|inner| {
        inner.jobs.iter().find(|row| row.id == id).map(|row| {
            (
                row.intents.cancelled.is_some(),
                row.intents.delete_history_after_cancel,
                row.report.retiring,
            )
        })
    })
}

pub fn clear_download_queue(pipeline: &mut Pipeline, id: JobId) {
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
}
pub fn report_status(pipeline: &mut Pipeline, id: JobId, status: JobStatus) {
    pipeline.jobs.get_mut(&id).unwrap().status = status;
    pipeline.report_job(id);
}
pub fn working_dir(pipeline: &Pipeline, id: JobId) -> &PathBuf {
    &pipeline.jobs[&id].working_dir
}
pub fn set_article_in_flight(pipeline: &mut Pipeline, id: JobId, active: bool) {
    if active {
        pipeline.articles_in_flight_by_job.insert(id, 1);
    } else {
        pipeline.articles_in_flight_by_job.remove(&id);
    }
}
pub fn set_active_decode(pipeline: &mut Pipeline, id: JobId, active: bool) {
    if active {
        pipeline.active_decodes_by_job.insert(id, 1);
    } else {
        pipeline.active_decodes_by_job.remove(&id);
    }
}
pub fn rename_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == id)
            .unwrap()
            .intents
            .rename_to
            .is_some()
    })
}
pub fn cleanup_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.pending_cleanup.contains(&id))
}
pub fn cleanup_or_history_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.pending_cleanup.contains(&id) || inner.history_operations.contains(&id))
}

#[derive(Debug, PartialEq, Eq)]
pub struct CheckpointVerdict(crate::jobs::feed_verdict::FeedVerdict);

pub fn save_feed_refusal(
    pipeline: &mut Pipeline,
    id: JobId,
    reason: String,
    file: u32,
) -> CheckpointVerdict {
    use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
    let mut verdict = crate::jobs::feed_verdict::FeedVerdict::new(reason);
    verdict.abandoned_files.insert(file);
    pipeline
        .db
        .save_graph_checkpoint(
            id,
            Arc::new(crate::jobs::graph_checkpoint::Delta {
                feed_verdict: Some(verdict.clone()),
                ..Default::default()
            }),
        )
        .unwrap();
    pipeline
        .post_probes
        .restore_verdict(id, Some(verdict.clone()));
    CheckpointVerdict(verdict)
}

pub fn recorded_verdict(pipeline: &Pipeline, id: JobId) -> Option<CheckpointVerdict> {
    use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
    pipeline
        .db
        .load_graph_checkpoint(id)
        .unwrap()
        .unwrap()
        .feed_verdict
        .map(CheckpointVerdict)
}
pub fn flush_checkpoint(pipeline: &mut Pipeline, id: JobId) {
    pipeline.flush_graph_checkpoint(id);
}
pub async fn finish_retiring_graph(pipeline: &mut Pipeline, id: JobId) {
    while pipeline.retiring_graph_jobs.contains_key(&id) {
        hear_graph_mail(pipeline, id).await;
    }
}
pub fn fail_job(pipeline: &mut Pipeline, id: JobId, error: String) {
    pipeline.fail_job(id, error);
}
pub fn has_feed_verdict(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.post_probes.verdicts.contains_key(&id)
}
pub fn is_dead_post(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.post_probes.dead.contains_key(&id)
}
pub fn download_queue_len(pipeline: &Pipeline, id: JobId) -> usize {
    pipeline.jobs[&id]
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}
pub fn apply_script_effects(pipeline: &mut Pipeline, id: JobId) -> bool {
    pipeline.apply_queue_script_effects(id)
}
pub fn report_job(pipeline: &mut Pipeline, id: JobId) {
    pipeline.report_job(id);
}
