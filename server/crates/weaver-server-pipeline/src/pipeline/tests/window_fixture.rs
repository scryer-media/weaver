// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
pub const QUEUED: u64 = 1_000;

pub async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    enqueue_named(pipeline, id, &format!("Window NZB {id}")).await;
}

pub async fn enqueue_named(pipeline: &mut Pipeline, id: u64, name: &str) {
    let spec = segmented_job_spec(name, "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("window-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Turns until the window has nothing left to install.
pub async fn settle(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

pub async fn long_queue(temp: &tempfile::TempDir, slots: u32) -> (Pipeline, SchedulerHandle) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    settle(&mut pipeline).await;
    (pipeline, handle)
}

pub fn installed(pipeline: &Pipeline) -> Vec<JobId> {
    let mut ids: Vec<JobId> = pipeline.jobs.keys().copied().collect();
    ids.sort_unstable();
    ids
}

pub fn rows(pipeline: &Pipeline) -> usize {
    pipeline.shared_state.queue.read(|inner| inner.jobs.len())
}

pub const QUEUE_WINDOW: usize = crate::pipeline::queue_window::QUEUE_WINDOW;
pub fn installed_count(pipeline: &Pipeline) -> usize {
    pipeline.jobs.len()
}
pub fn has_installed(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.jobs.contains_key(&id)
}
pub fn admitted(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.nzb_admitted.contains(&id)
}
pub fn parked(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.window_parked(id)
}
pub fn set_progress(pipeline: &mut Pipeline, id: JobId, downloaded: u64) {
    pipeline.jobs.get_mut(&id).unwrap().downloaded_bytes = downloaded;
}
pub fn checkpoint_job(pipeline: &mut Pipeline, id: JobId) {
    pipeline.checkpoint_graph_job(id);
}
pub fn has_checkpoint(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.db.has_graph_checkpoint(id).unwrap()
}
pub fn progress_and_status(pipeline: &Pipeline, id: JobId) -> Option<(u64, JobStatus)> {
    pipeline
        .jobs
        .get(&id)
        .map(|s| (s.downloaded_bytes, s.status.clone()))
}
pub fn on_disk(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.job(id).unwrap().report.on_disk)
}
pub fn unreadable_job(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.window_unreadable.contains(&id)
}
pub fn retiring(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.job(id).is_some_and(|row| row.report.retiring))
}

pub fn corrupt_parked_nzb(pipeline: &Pipeline, id: JobId) {
    let datastore = pipeline.db.datastore();
    pipeline
        .db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE job_nzbs SET nzb_zstd = {} WHERE job_id = {}",
                &[
                    SqlArg::Bytes(b"not an nzb".to_vec()),
                    SqlArg::I64(id.0 as i64),
                ],
            )
            .await?;
            SqlRuntime::execute(
                datastore.write_exec(),
                // With no stored outline, its entry reads the NZB.
                "UPDATE active_jobs SET nzb_files = NULL WHERE job_id = {}",
                &[SqlArg::I64(id.0 as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();
}

pub fn intermediate_dir(pipeline: &Pipeline) -> &Path {
    &pipeline.intermediate_dir
}

pub fn articles_queued(pipeline: &Pipeline, id: JobId) -> bool {
    matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Queued(_)
    )
}

pub fn articles_active(pipeline: &Pipeline, id: JobId) -> bool {
    matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Active(_)
    )
}

pub fn move_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.moving_jobs.contains_key(&id)
}

pub fn no_pending_moves(pipeline: &Pipeline) -> bool {
    pipeline.moving_jobs.is_empty()
}

pub fn awaits_articles(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.awaits_job_articles(id)
}

pub fn request_activation(pipeline: &mut Pipeline, id: JobId) {
    pipeline.request_job_activation(id);
}

pub struct ActivationReply(activation::JobActivated);

impl ActivationReply {
    pub async fn receive(pipeline: &mut Pipeline) -> Self {
        Self(pipeline.job_activated_rx.recv().await.unwrap())
    }

    pub fn deliver(self, pipeline: &mut Pipeline) {
        pipeline.handle_job_activated(self.0);
    }

    pub fn moved_to(&self) -> Option<&Path> {
        self.0.moved_to.as_deref()
    }

    pub fn take_moved_dir(&self, pipeline: &mut Pipeline) -> bool {
        pipeline.take_moved_dir(
            self.0.job_id,
            &self.0.started_in,
            self.0.moved_to.as_deref().expect("the read moved the job"),
        )
    }
}

pub fn uninstall_job(pipeline: &mut Pipeline, id: JobId) -> crate::jobs::model::JobState {
    pipeline.jobs.remove(&id).unwrap()
}

pub fn is_job_working_dir(root: &Path, dir: &Path, id: JobId) -> std::io::Result<bool> {
    crate::jobs::working_dir::is_job_working_dir(root, dir, id)
}

pub fn resume_stored(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.resumes_stored.contains(&id)
}

// One queued job that never started, its intermediate folder then changed.
pub async fn unstarted_job_with_a_changed_folder(
    temp: &tempfile::TempDir,
    id: JobId,
) -> (Pipeline, PathBuf, PathBuf) {
    unstarted_job_named_with_a_changed_folder(temp, id, &format!("Window NZB {}", id.0)).await
}

pub async fn unstarted_job_named_with_a_changed_folder(
    temp: &tempfile::TempDir,
    id: JobId,
    name: &str,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, old_root, complete) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    enqueue_named(&mut pipeline, id.0, name).await;
    settle(&mut pipeline).await;
    assert!(matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Queued(_)
    ));
    let old_dir = pipeline.jobs[&id].working_dir.clone();
    assert_eq!(old_dir.parent(), Some(old_root.as_path()));
    let new_root = temp.path().join("moved-intermediate");
    handle
        .update_runtime_paths(temp.path().join("data"), new_root.clone(), complete)
        .await
        .unwrap();
    settle(&mut pipeline).await;
    (pipeline, old_dir, new_root)
}

pub fn reported_output_dir(pipeline: &Pipeline, id: JobId) -> Option<String> {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.row(id).unwrap().report.job.output_dir.clone())
}

pub struct MoveGate(Arc<activation::MoveGate>);

impl MoveGate {
    pub fn hold_move(pipeline: &mut Pipeline) -> Self {
        let gate = Arc::new(activation::MoveGate::default());
        pipeline.move_gates.moving = Some(Arc::clone(&gate));
        Self(gate)
    }

    pub fn hold_removal(pipeline: &mut Pipeline) -> Self {
        let gate = Arc::new(activation::MoveGate::default());
        pipeline.move_gates.leaving = Some(Arc::clone(&gate));
        Self(gate)
    }

    pub async fn reached(&self) {
        self.0.reached().await;
    }

    pub fn open(&self) {
        self.0.open();
    }
}

impl Drop for MoveGate {
    fn drop(&mut self) {
        self.open();
    }
}

pub struct MoveSeams(Arc<activation::move_seams::Seams>);

impl MoveSeams {
    pub fn hold_move(root: &Path, listing: bool) -> Self {
        use activation::move_seams::{self, Gate, Seams};
        Self(move_seams::arm(
            root,
            Seams {
                listing: listing.then(Gate::default),
                ..Default::default()
            },
        ))
    }

    pub fn hold_staging_removal(root: &Path) -> Self {
        use activation::move_seams::{self, Gate, Seams};
        Self(move_seams::arm(
            root,
            Seams {
                staging_removal: Some(Gate::default()),
                ..Default::default()
            },
        ))
    }

    pub fn claimed(&self) -> bool {
        self.0.claimed.load(Ordering::Acquire)
    }

    pub fn listed(&self) -> bool {
        self.0.listed.load(Ordering::Acquire)
    }

    pub fn abandoned(&self) -> bool {
        self.0.abandoned.load(Ordering::Acquire)
    }

    pub fn cleanup_waited(&self) -> bool {
        self.0.cleanup_waited.load(Ordering::Acquire)
    }

    pub fn staging_removed(&self) -> bool {
        self.0.staging_removed.load(Ordering::Acquire)
    }

    pub fn open_row(&self) {
        self.0.row.open();
    }

    pub fn open_abandon(&self) {
        self.0.abandon.open();
    }

    pub fn open_listing(&self) {
        self.0.listing.as_ref().unwrap().open();
    }

    pub fn open_staging_removal(&self) {
        self.0.staging_removal.as_ref().unwrap().open();
    }

    pub fn disarm(self) {
        activation::move_seams::disarm();
    }
}

pub async fn retry_waits_or_clears(pipeline: &Pipeline) -> (usize, usize) {
    let queue = pipeline.shared_state.queue.clone();
    let mut revision = queue.subscribe();
    loop {
        let seen = queue.read(|inner| (inner.waits_on_moves, inner.restarts_clearing));
        if seen != (0, 0) {
            return seen;
        }
        revision.changed().await.unwrap();
    }
}

pub async fn wait_for_move_cleanup(pipeline: &Pipeline, id: JobId) {
    let mut revision = pipeline.shared_state.queue.subscribe();
    while pipeline
        .shared_state
        .queue
        .read(|inner| inner.moves_out.contains_key(&id) || inner.pending_cleanup.contains(&id))
    {
        revision.changed().await.unwrap();
    }
}

pub fn mark_started(pipeline: &mut Pipeline, id: JobId) {
    pipeline.jobs.get_mut(&id).unwrap().failed_bytes = 1;
}

pub fn cancel_folders_held(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.pending_cleanup.folders_held(&id))
}

pub fn has_queue_row(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.row(id).is_some())
}

pub fn hold_move_owner(pipeline: &Pipeline, id: JobId, held_by_history: bool) {
    pipeline.shared_state.queue.write(|inner| {
        if held_by_history {
            inner.history_operations.insert(id);
        } else {
            inner.admissions.insert(id);
        }
        crate::pipeline::queue::Change::unchanged(())
    });
}

pub fn release_move_owner(pipeline: &Pipeline, id: JobId) {
    pipeline.shared_state.queue.write(|inner| {
        inner.history_operations.remove(&id);
        inner.admissions.remove(&id);
        crate::pipeline::queue::Change::unchanged(())
    });
}

pub fn gate_parked_category_edit(pipeline: &Pipeline, id: JobId) -> std::sync::mpsc::Sender<()> {
    let (open, gate) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_job_write(id, "test_gated_edit", move |db| {
            gate.recv().unwrap();
            db.update_active_job(
                id,
                &crate::jobs::JobUpdate {
                    category: crate::jobs::model::FieldUpdate::Set("fresh".into()),
                    ..Default::default()
                },
            )
        })
        .unwrap();
    open
}

pub fn parked_write_waits() -> u64 {
    crate::persistence::connection::JOB_WRITE_WAITS.load(Ordering::Relaxed)
}

pub async fn read_parked(
    pipeline: &Pipeline,
    id: JobId,
) -> Vec<(JobId, Result<Option<RestoreJobRequest>, String>)> {
    pipeline.parked_loads(vec![id]).read().await
}

pub fn added_script_raises() -> u64 {
    crate::pipeline::orchestrator::ADDED_SCRIPT_RAISES.load(Ordering::Relaxed)
}
