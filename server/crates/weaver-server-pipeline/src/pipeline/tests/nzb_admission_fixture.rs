// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::queue_fixture::handle;
use super::*;
use crate::jobs::model::FileSummaries;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::jobs::queries::JobsInternalQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;

pub async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 8]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("queued-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Scripts for every queue event, whose script file is gone so that each run
// ends as soon as its turn comes.
pub fn enable_arrival_scripts(pipeline: &Pipeline, root: &std::path::Path) {
    super::script_fixture::enable_queue_script(&pipeline.db, root);
    std::fs::remove_file(
        pipeline
            .db
            .post_processing_script_directory()
            .unwrap()
            .join("queue.sh"),
    )
    .unwrap();
}

// Lets the pipeline release every arrival hold whose scripts end, until the
// holds of `ids` have all been released.
pub async fn finish_added_scripts(pipeline: &mut Pipeline, ids: &[JobId]) {
    let mut waiting: HashSet<_> = ids.iter().copied().collect();
    while !waiting.is_empty() {
        if let TerminalPostProcessingEvent::AddedScriptsDone(id, result) = pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            pipeline.handle_added_scripts_done(id, result);
            waiting.remove(&id);
        }
    }
}

// Adds a job whose spec is the one its saved NZB parses to, so a restart
// restores the same files.
pub async fn enqueue_saved(pipeline: &mut Pipeline, id: u64) {
    let nzb_zstd = sample_nzb_zstd();
    let path = PathBuf::from(format!("queued-{id}.nzb"));
    let nzb = crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd).unwrap();
    let spec = crate::ingest::nzb_to_spec(&nzb, &path, None, Vec::new());
    pipeline
        .add_job(
            JobId(id),
            spec,
            path,
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Restarts the pipeline over the same data and hands it every saved job,
// with the NZB cap set to one.
pub async fn restart_and_restore(temp: &tempfile::TempDir, pipeline: Pipeline) -> Pipeline {
    retire_pipeline_database(pipeline).await;
    let (pipeline, intermediate, _) = new_direct_pipeline(temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle(&pipeline)
            .restore_job(candidate.request)
            .await
            .unwrap();
    }
    pipeline
}

pub fn configure_providers(pipeline: &mut Pipeline) {
    let mut config = weaver_nntp::client::NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port: 1,
            tls: false,
            ..Default::default()
        },
        10,
    );
    config.servers = (0..24)
        .map(|_| {
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".into(),
                    port: 1,
                    tls: false,
                    ..Default::default()
                },
                10,
            )
            .servers
            .remove(0)
        })
        .collect();
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(config));
    pipeline.tuner.set_connection_limit(240);
}

pub async fn deliver_fetch_failure(
    pipeline: &mut Pipeline,
    job_id: JobId,
    error: &weaver_nntp::NntpError,
) {
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = work.dispatched(&pipeline.jobs[&job_id].spec);
    let segment_id = work.segment_id;
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Fetch(
                DownloadFailure::from_lane_acquire_failure(Some(error)),
            )),
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

pub fn mark_reprocess(pipeline: &mut Pipeline, id: JobId) {
    // Model the prepared history-reprocess ticket before the actor installs it.
    pipeline.shared_state.queue.write(|inner| {
        let row = inner.jobs.iter_mut().find(|row| row.id == id).unwrap();
        let prepared = row.admission.as_mut().unwrap();
        prepared.restart = Some(crate::pipeline::queue::RestartAdmission {
            reprocess: true,
            working_dir: prepared.working_dir.clone(),
            downloaded_bytes: prepared.spec.total_bytes,
        });
        crate::pipeline::queue::Change::changed(())
    });
}

pub async fn prepare_add(
    pipeline: &Pipeline,
    id: JobId,
    spec: JobSpec,
    path: PathBuf,
    nzb: Vec<u8>,
) {
    pipeline
        .shared_state
        .queue
        .clone()
        .add(
            id,
            spec,
            path,
            nzb,
            crate::jobs::AddJobOptions::default(),
            pipeline.event_tx.clone(),
        )
        .await
        .unwrap();
}

pub async fn add_lean(
    pipeline: &mut Pipeline,
    id: JobId,
    spec: JobSpec,
    path: PathBuf,
    nzb: Vec<u8>,
) {
    pipeline
        .add_job(id, spec, path, nzb, crate::jobs::AddJobOptions::default())
        .await
        .unwrap();
}

pub fn reprocess_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.nzb_reprocess_pending.contains(&id)
}

pub fn queued_download_len(pipeline: &Pipeline, id: JobId) -> usize {
    pipeline.jobs[&id].articles.queued_download_len()
}

pub fn graph_awaits_mail(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .graph_jobs
        .get(&id)
        .is_some_and(|job| job.awaits_mail())
}

pub fn note_seen(pipeline: &mut Pipeline, id: JobId) {
    pipeline.nzb_seen.insert(id);
}

pub fn note_pause_release(pipeline: &mut Pipeline, id: JobId) {
    pipeline.nzb_pause_releases.insert(id);
}

pub fn note_resume(pipeline: &mut Pipeline, id: JobId) {
    pipeline.resumes_stored.insert(id);
}

pub fn set_move_in_flight(pipeline: &mut Pipeline, id: JobId, moving: bool) {
    if moving {
        pipeline.inflight_moves.insert(id);
    } else {
        pipeline.inflight_moves.remove(&id);
    }
}

pub fn reconcile(pipeline: &mut Pipeline, walk: bool) -> bool {
    pipeline.reconcile_nzb_admission(walk)
}

pub fn walk_due(pipeline: &Pipeline) -> bool {
    pipeline.queue_walk_due
}

pub fn seen(pipeline: &Pipeline) -> &HashSet<JobId> {
    &pipeline.nzb_seen
}

pub fn pause_releases(pipeline: &Pipeline) -> &HashSet<JobId> {
    &pipeline.nzb_pause_releases
}

pub fn retired_busy(pipeline: &Pipeline) -> &HashSet<JobId> {
    &pipeline.nzb_retired_busy
}

pub fn stored_resumes(pipeline: &Pipeline) -> &HashSet<JobId> {
    &pipeline.resumes_stored
}

pub fn admissions(pipeline: &Pipeline) -> &HashSet<JobId> {
    &pipeline.nzb_admitted
}

pub fn wait_for_volumes(pipeline: &mut Pipeline, id: JobId) {
    pipeline.jobs.get_mut(&id).unwrap().post_state = crate::PostState::WaitingForVolumes;
}

pub fn queue_infos(pipeline: &Pipeline) -> Vec<JobInfo> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .map(|row| row.info(&inner.globals))
            .collect()
    })
}

pub fn persisted_pause_releases(pipeline: &Pipeline) -> HashSet<JobId> {
    pipeline.db.load_nzb_pause_releases().unwrap()
}

pub fn persist_pause_release(pipeline: &Pipeline, id: JobId) {
    pipeline.db.set_nzb_pause_release(id, true).unwrap();
}

pub fn holds_arrival(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.held_for_added_scripts(id)
}

pub fn report_is_restoring(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.job(id).unwrap().report.restoring)
}

pub fn queue_info(pipeline: &Pipeline, id: JobId) -> JobInfo {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.job(id).unwrap().info(&inner.globals))
}

pub fn hold_origin(pipeline: &Pipeline) -> Option<crate::jobs::HoldOrigin> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .globals
            .holds
            .reason(crate::jobs::HoldOwner::All, false)
    })
}

pub fn mark_shutting_down(pipeline: &Pipeline) {
    pipeline.shared_state.queue.write(|inner| {
        inner.globals.shutting_down = true;
        crate::pipeline::queue::Change::changed(())
    });
}

pub fn prepared_add(pipeline: &Pipeline, id: JobId) -> (JobSpec, FileSummaries, JobInfo) {
    pipeline.shared_state.queue.read(|inner| {
        let row = inner.job(id).unwrap();
        let add = row.admission.as_ref().unwrap();
        (
            add.spec.clone(),
            add.summaries.clone(),
            row.report.job.clone(),
        )
    })
}

pub fn counts_par2_bytes(pipeline: &Pipeline, id: JobId) -> u64 {
    pipeline.jobs[&id].par2_bytes
}
