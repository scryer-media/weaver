// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::control_fixture::CompletedMove;
use super::*;

pub async fn enable_delivery_renaming(pipeline: &mut Pipeline) {
    pipeline.config.write().await.delivery_naming =
        Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(true),
            enable_srrdb_lookup: None,
        });
}

pub fn install_delivery_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    working_dir: PathBuf,
) {
    pipeline
        .jobs
        .insert(job_id, minimal_job_state(job_id, job_name, working_dir));
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
}

pub async fn finish_delivery(pipeline: &mut Pipeline, job_id: JobId) -> (PathBuf, bool) {
    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    let succeeded = done.result.is_ok();
    pipeline.handle_move_to_complete_done(done).await;
    settle_script_admissions(pipeline).await;

    (dest, succeeded)
}

pub fn cover_with_par2(pipeline: &mut Pipeline, id: JobId) {
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.par2_bytes = state.spec.total_bytes;
}
pub fn reconcile_delivery(pipeline: &mut Pipeline, id: JobId) -> Result<(), String> {
    pipeline.reconcile_terminal_delivery(id)
}
pub fn is_reconciled(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.terminal_reconciliations.contains_key(&id)
}
pub fn terminal_cause(
    pipeline: &Pipeline,
    id: JobId,
) -> Option<&crate::jobs::SemanticTerminalCause> {
    pipeline.semantic_terminal_causes.get(&id)
}

// Runs the delivered one-article job up to its final move and hands back
// the move's done message, unhandled: what a restart finds is what the move
// left, with the terminal pass never begun.
pub async fn move_without_hearing_it(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    payload: &[u8],
) -> (PathBuf, CompletedMove) {
    let working_dir =
        insert_delivered_job(pipeline, job_id, job_name, "payload.bin", payload).await;
    tokio::fs::write(working_dir.join("sidecar.nfo"), b"sidecar")
        .await
        .unwrap();
    // The job's working directory carries its ownership marker, as every
    // directory a job is added with does.
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, job_id)
        .await
        .unwrap();
    until_the_move(pipeline, job_id, working_dir).await
}

// Runs a job whose every article is in hand up to its final move and hands
// back the move's done message, unhandled.
pub async fn until_the_move(
    pipeline: &mut Pipeline,
    job_id: JobId,
    working_dir: PathBuf,
) -> (PathBuf, CompletedMove) {
    pipeline.check_job_completion(job_id).await;
    // The download's queue-script barrier runs no script and lets it on.
    while !pipeline.inflight_moves.contains(&job_id) {
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            continue;
        }
        match pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::QueueAdmitted(id) => {
                pipeline.handle_queue_scripts_admitted(id);
            }
            TerminalPostProcessingEvent::QueueDone(id, result) => {
                pipeline.handle_queue_scripts_done(id, result).await;
            }
            _ => panic!("no script runs before the move"),
        }
    }
    let done = CompletedMove::receive(pipeline).await;
    assert_eq!(done.job_id(), job_id);
    (working_dir, done)
}

// Restarts on the same directories and restores the job as startup does.
pub async fn restart_and_restore(
    temp: &tempfile::TempDir,
    intermediate_dir: &std::path::Path,
    job_id: JobId,
) -> Pipeline {
    let (restored, _, _) = new_direct_pipeline(temp).await;
    restore_into(restored, temp, intermediate_dir, job_id).await
}

// Restores the job into `restored`, opened on the same directories, as
// startup does.
pub async fn restore_into(
    mut restored: Pipeline,
    temp: &tempfile::TempDir,
    intermediate_dir: &std::path::Path,
    job_id: JobId,
) -> Pipeline {
    let startup = crate::operations::recovery::recover_server_state(
        &restored.db,
        &temp.path().join("data"),
        intermediate_dir,
    )
    .await
    .unwrap();
    let request = startup
        .to_restore
        .into_iter()
        .find(|candidate| candidate.job_id == job_id)
        .expect("the job must come back as in-progress work")
        .request;
    assert_eq!(request.status, JobStatus::Moving);
    restored.restore_job(request).await.unwrap();
    // Every article is in hand: nothing is queued to fetch and no graph
    // is restored from a checkpoint. A job with nothing left to move may
    // already have finished.
    assert!(
        restored.jobs.get(&job_id).is_none_or(|state| matches!(
            state.articles,
            crate::jobs::model::JobArticles::Queued(_)
        ))
    );
    assert!(!restored.graph_jobs.contains_key(&job_id));
    restored
}

pub fn before_first_move(pipeline: &Pipeline, hook: impl FnOnce(JobId) + Send + 'static) {
    crate::pipeline::completion::finalize::FIXTURE_MOVE_HOOKS
        .lock()
        .unwrap()
        .insert(pipeline.complete_dir.clone(), Box::new(hook));
}

pub fn before_spent_removal(pipeline: &Pipeline, hook: impl FnOnce(&[PathBuf]) + Send + 'static) {
    crate::pipeline::completion::finalize::FIXTURE_SPENT_HOOKS
        .lock()
        .unwrap()
        .insert(pipeline.complete_dir.clone(), Box::new(hook));
}

pub fn move_payload_first(pipeline: &Pipeline, intermediate_dir: &Path, destination: &Path) {
    let first_moved = destination.join("payload.bin");
    let intermediate = intermediate_dir.to_path_buf();
    before_first_move(pipeline, move |job_id| {
        let working_dir = crate::jobs::working_dir::find_job_working_dir(&intermediate, job_id)
            .unwrap()
            .unwrap();
        std::fs::rename(working_dir.join("payload.bin"), &first_moved).unwrap();
    });
}

pub fn drain_delivery_work(pipeline: &mut Pipeline, job_id: JobId) -> Vec<DownloadWork> {
    let active = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap();
    let mut works = active.download_queue.drain_all();
    works.extend(active.recovery_queue.drain_all());
    let spec = &pipeline.jobs[&job_id].spec;
    works
        .into_iter()
        .map(|item| item.dispatched(spec))
        .collect()
}

pub async fn deliver_posts(
    pipeline: &mut Pipeline,
    job_id: JobId,
    works: &[DownloadWork],
    posts: &[super::engines::harness::Post],
) {
    pipeline.hold_test_lane_works(works.to_vec(), false);
    for work in works {
        let post = &posts[work.segment_id.file_id.file_index as usize];
        let raw = encode_article_part(&post.name, &post.bytes, 1, 1, 1, post.bytes.len() as u64);
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id: work.segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
    }
    drain_decode_results(pipeline, works.len()).await;
    hear_graph_mail(pipeline, job_id).await;
}
