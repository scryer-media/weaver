// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue far longer than its window. Only the window's jobs are installed;
// every other job is its row and its stored rows.

use super::*;
use crate::jobs::handle::QueueMoveTarget;
use crate::pipeline::queue_window::QUEUE_WINDOW;

const QUEUED: u64 = 1_000;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Window NZB {id}"), "payload.bin", &[512; 4]);
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
async fn settle(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

async fn long_queue(temp: &tempfile::TempDir, slots: u32) -> (Pipeline, SchedulerHandle) {
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

fn installed(pipeline: &Pipeline) -> Vec<JobId> {
    let mut ids: Vec<JobId> = pipeline.jobs.keys().copied().collect();
    ids.sort_unstable();
    ids
}

fn rows(pipeline: &Pipeline) -> usize {
    pipeline.shared_state.queue.read(|inner| inner.jobs.len())
}

#[tokio::test]
async fn a_long_queue_installs_its_first_window_and_a_priority_raise_moves_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW);
    assert_eq!(
        installed(&pipeline),
        (1..=QUEUE_WINDOW as u64).map(JobId).collect::<Vec<_>>()
    );
    assert_eq!(rows(&pipeline), QUEUED as usize);

    // Job 900 raised to high priority enters; the last job of the window
    // leaves it, its row kept.
    handle
        .update_job(
            JobId(900),
            crate::jobs::JobUpdate {
                metadata_edits: vec![("priority".into(), Some("high".into()))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW);
    assert!(pipeline.jobs.contains_key(&JobId(900)));
    assert!(!pipeline.jobs.contains_key(&JobId(QUEUE_WINDOW as u64)));
    assert_eq!(rows(&pipeline), QUEUED as usize);
    assert!(
        handle
            .get_job(JobId(QUEUE_WINDOW as u64))
            .is_ok_and(|job| job.status == JobStatus::Queued)
    );
}

#[tokio::test]
async fn slot_holders_stay_installed_wherever_they_stand() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 3).await;
    let holders = [JobId(1), JobId(2), JobId(3)];
    assert!(holders.iter().all(|id| pipeline.nzb_admitted.contains(id)));
    handle
        .reorder_jobs(
            holders
                .iter()
                .map(|id| (*id, QueueMoveTarget::Bottom))
                .collect(),
        )
        .await
        .unwrap();
    settle(&mut pipeline).await;
    // The window is the first rows in queue order, the holders beside it.
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW + holders.len());
    assert!(holders.iter().all(|id| pipeline.jobs.contains_key(id)));
    assert!((4..4 + QUEUE_WINDOW as u64).all(|id| pipeline.jobs.contains_key(&JobId(id))));

    // With the window paused, the job a freed slot admits is past it: it is
    // installed as it takes the slot.
    let window: Vec<JobId> = (4..4 + QUEUE_WINDOW as u64).map(JobId).collect();
    let next = JobId(4 + QUEUE_WINDOW as u64);
    assert!(!pipeline.jobs.contains_key(&next));
    for outcome in handle.set_jobs_paused(window, true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    handle.cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(pipeline.nzb_admitted.contains(&next));
    assert!(pipeline.jobs.contains_key(&next));
    assert!(!pipeline.jobs.contains_key(&JobId(1)));
}

#[tokio::test]
async fn each_add_raises_its_arrival_script_once() {
    use crate::pipeline::orchestrator::ADDED_SCRIPT_RAISES;
    const ADDS: u64 = 500;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    super::script_events::enable_queue_script(&pipeline.db, temp.path());
    let raised = ADDED_SCRIPT_RAISES.load(Ordering::Relaxed);
    for id in 1..=ADDS {
        enqueue(&mut pipeline, id).await;
    }
    settle(&mut pipeline).await;
    assert_eq!(ADDED_SCRIPT_RAISES.load(Ordering::Relaxed) - raised, ADDS);
    assert_eq!(rows(&pipeline), ADDS as usize);
    // Jobs entering and leaving the window raise nothing again.
    let mut ids: Vec<JobId> = (1..=ADDS).map(JobId).collect();
    ids.reverse();
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .reorder_jobs(
            ids.into_iter()
                .take(QUEUE_WINDOW)
                .map(|id| (id, QueueMoveTarget::Top))
                .collect(),
        )
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(ADDED_SCRIPT_RAISES.load(Ordering::Relaxed) - raised, ADDS);
}

#[tokio::test]
async fn a_restart_installs_only_the_window_and_lists_every_row_in_place() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    assert_eq!(recovered.to_restore.len(), QUEUED as usize);
    let order: Vec<JobId> = recovered.to_restore.iter().map(|c| c.job_id).collect();
    assert_eq!(order, (1..=QUEUED).map(JobId).collect::<Vec<_>>());
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle(&mut pipeline).await;
    assert_eq!(
        installed(&pipeline),
        (1..=QUEUE_WINDOW as u64).map(JobId).collect::<Vec<_>>()
    );
    // Every row is listed, in the order the restart read.
    let listed: Vec<JobId> = handle.list_jobs().iter().map(|job| job.job_id).collect();
    assert_eq!(listed, order);
    assert!(
        handle
            .list_jobs()
            .iter()
            .all(|job| job.status == JobStatus::Queued
                || pipeline.nzb_admitted.contains(&job.job_id))
    );
}

// Jobs paused before they started wait as their rows: the window holds the
// jobs behind them instead, a restart installs none of them, and a resumed
// one enters on its turn. A paused job that started stays installed.
#[tokio::test]
async fn jobs_paused_before_they_start_take_no_window_place() {
    let paused_ids = || 3..=62u64;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    // The started job has bytes in hand and a stored checkpoint, as a job
    // that fetched articles does.
    let started = JobId(63);
    activate_job(&mut pipeline, started).await;
    pipeline.jobs.get_mut(&started).unwrap().downloaded_bytes = 512;
    pipeline.checkpoint_graph_job(started);
    pipeline.flush_graph_checkpoint(started);
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(pipeline.db.has_graph_checkpoint(started).unwrap());
    let paused: Vec<JobId> = paused_ids().map(JobId).chain([started]).collect();
    for outcome in handle.set_jobs_paused(paused, true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    // The slot holders, the started job, and the first jobs that are not
    // paused, a full window of them.
    let window = |last: u64| -> Vec<JobId> {
        [1, 2]
            .into_iter()
            .chain(63..=last)
            .map(JobId)
            .collect::<Vec<_>>()
    };
    assert_eq!(installed(&pipeline), window(160));
    assert!(paused_ids().all(|id| pipeline.window_parked(JobId(id))));
    assert_eq!(rows(&pipeline), QUEUED as usize);
    assert!(paused_ids().all(|id| {
        handle
            .get_job(JobId(id))
            .is_ok_and(|job| job.status == JobStatus::Paused)
    }));

    // Resumed, a job enters in its place in the order; the last of the
    // window leaves.
    let resumed = JobId(10);
    for outcome in handle.set_jobs_paused([resumed], false).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    let mut expected = window(159);
    expected.insert(2, resumed);
    assert_eq!(installed(&pipeline), expected);
    assert!(!pipeline.window_parked(resumed));

    // A restart installs none of the paused jobs either.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
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
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle(&mut pipeline).await;
    let installed_now = installed(&pipeline);
    assert!(
        paused_ids()
            .filter(|id| *id != resumed.0)
            .all(|id| !installed_now.contains(&JobId(id))),
        "a paused job that never started was installed: {installed_now:?}"
    );
    // The started job keeps its place in the window across the restart.
    assert!(
        installed_now.contains(&started),
        "the paused job that started lost its place: {installed_now:?}"
    );
    assert!(!pipeline.window_parked(started));
    assert_eq!(rows(&pipeline), QUEUED as usize);
}

// A paused job that started, moved out of the window and back, is read back
// with its stored checkpoint: its row says it started, so it keeps the place
// it was moved to, turn after turn.
#[tokio::test]
async fn a_started_paused_job_moved_out_and_back_keeps_its_place() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let started = JobId(63);
    activate_job(&mut pipeline, started).await;
    pipeline.jobs.get_mut(&started).unwrap().downloaded_bytes = 512;
    pipeline.checkpoint_graph_job(started);
    pipeline.flush_graph_checkpoint(started);
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(pipeline.db.has_graph_checkpoint(started).unwrap());
    for outcome in handle.set_jobs_paused([started], true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    assert!(installed(&pipeline).contains(&started));

    // A restart restores it from its checkpoint, with nothing in hand.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
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
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle(&mut pipeline).await;
    assert!(installed(&pipeline).contains(&started));

    handle
        .reorder_jobs(vec![(started, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert!(
        pipeline.window_parked(started),
        "the moved job stayed in the window: {:?}",
        pipeline
            .jobs
            .get(&started)
            .map(|s| (s.downloaded_bytes, s.status.clone()))
    );

    handle
        .reorder_jobs(vec![(started, QueueMoveTarget::Top)])
        .await
        .unwrap();
    settle(&mut pipeline).await;
    let on_disk = |pipeline: &Pipeline| {
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.job(started).unwrap().report.on_disk)
    };
    assert!(
        on_disk(&pipeline),
        "the re-entered row states its checkpoint"
    );
    for _ in 0..3 {
        assert!(installed(&pipeline).contains(&started));
        assert!(!pipeline.window_parked(started));
        settle(&mut pipeline).await;
    }
    assert_eq!(handle.get_job(started).unwrap().status, JobStatus::Paused);
}

// The intermediate folder changes with two jobs queued, and Weaver restarts
// before either runs on. The job that started keeps its working directory;
// the one that never started works under the new folder as it starts, and
// leaves nothing behind in the old one. Another restart keeps both answers.
#[tokio::test]
async fn an_intermediate_folder_change_moves_only_the_jobs_that_never_started() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, old_root, complete) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let (started, unstarted) = (JobId(1), JobId(2));
    enqueue(&mut pipeline, started.0).await;
    enqueue(&mut pipeline, unstarted.0).await;
    settle(&mut pipeline).await;
    activate_job(&mut pipeline, started).await;
    pipeline.jobs.get_mut(&started).unwrap().downloaded_bytes = 512;
    pipeline.checkpoint_graph_job(started);
    pipeline.flush_graph_checkpoint(started);
    pipeline.db.flush_write_queue().await.unwrap();
    let working = |pipeline: &Pipeline, id: JobId| pipeline.jobs[&id].working_dir.clone();
    let (started_dir, unstarted_dir) = (working(&pipeline, started), working(&pipeline, unstarted));
    assert_eq!(started_dir.parent(), Some(old_root.as_path()));
    assert_eq!(unstarted_dir.parent(), Some(old_root.as_path()));
    assert!(unstarted_dir.exists());

    // The setting changes: nothing moves yet.
    let new_root = temp.path().join("moved-intermediate");
    let data = temp.path().join("data");
    handle
        .update_runtime_paths(data.clone(), new_root.clone(), complete.clone())
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(pipeline.intermediate_dir, new_root);
    assert_eq!(working(&pipeline, unstarted), unstarted_dir);
    assert!(unstarted_dir.exists());

    let restart = |pipeline: Pipeline| {
        let (data, new_root, complete) = (data.clone(), new_root.clone(), complete.clone());
        let db_path = temp.path().join("weaver.db");
        async move {
            retire_pipeline_database(pipeline).await;
            let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
                data.clone(),
                new_root.clone(),
                complete,
                db_path,
                16 * 1024 * 1024,
                0,
                None,
            )
            .await;
            let handle =
                SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
            let recovered = crate::operations::recover_server_state(&pipeline.db, &data, &new_root)
                .await
                .unwrap();
            let mut dirs = std::collections::BTreeMap::new();
            for candidate in recovered.to_restore {
                dirs.insert(candidate.job_id, candidate.request.working_dir.clone());
                handle.restore_job(candidate.request).await.unwrap();
            }
            settle(&mut pipeline).await;
            (pipeline, dirs)
        }
    };
    let (mut pipeline, _) = restart(pipeline).await;
    // The started job comes back with its checkpoint already built; the
    // other waits for its first handout.
    for id in [started, unstarted] {
        if matches!(
            pipeline.jobs[&id].articles,
            crate::jobs::model::JobArticles::Queued(_)
        ) {
            activate_job(&mut pipeline, id).await;
        }
    }
    assert!(matches!(
        pipeline.jobs[&unstarted].articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    assert_eq!(working(&pipeline, started), started_dir);
    assert!(started_dir.exists());
    let moved = working(&pipeline, unstarted);
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(crate::jobs::working_dir::is_job_working_dir(&new_root, &moved, unstarted).unwrap());
    assert!(!unstarted_dir.exists(), "the old folder is left behind");

    // A second restart reads each job's directory back as it is.
    let (pipeline, dirs) = restart(pipeline).await;
    assert_eq!(dirs[&started], started_dir);
    assert_eq!(dirs[&unstarted], moved);
    drop(pipeline);
}

// A job that never started is sent for, let go, and sent for again while the
// first read is still moving its working directory. The second ask waits
// for that move to be heard, so the job gets one new directory, not two.
#[tokio::test]
async fn a_job_sent_for_twice_moves_its_working_directory_once() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, old_root, complete) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    let queued = |pipeline: &Pipeline| {
        matches!(
            pipeline.jobs[&id].articles,
            crate::jobs::model::JobArticles::Queued(_)
        )
    };
    assert!(queued(&pipeline));
    let old_dir = pipeline.jobs[&id].working_dir.clone();
    assert_eq!(old_dir.parent(), Some(old_root.as_path()));

    let new_root = temp.path().join("moved-intermediate");
    handle
        .update_runtime_paths(temp.path().join("data"), new_root.clone(), complete)
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(pipeline.intermediate_dir, new_root);

    // The first ask has the move; the job is let go before its reply.
    pipeline.request_job_activation(id);
    assert!(pipeline.moving_jobs.contains_key(&id));
    pipeline.release_job_articles(id);
    assert!(queued(&pipeline));
    // The second ask sends nothing while the move is out.
    pipeline.request_job_activation(id);
    assert!(queued(&pipeline));
    assert!(pipeline.awaits_job_articles(id));

    // The first reply is no longer the job's activation, and its move
    // stands all the same.
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    pipeline.handle_job_activated(done);
    assert!(queued(&pipeline));
    assert!(pipeline.moving_jobs.is_empty());
    let moved = pipeline.jobs[&id].working_dir.clone();
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(!old_dir.exists(), "the old folder is left behind");
    // The stored row named the new directory before the old one went.
    let stored = |pipeline: &Pipeline| {
        pipeline
            .db
            .load_active_job_row(id)
            .unwrap()
            .unwrap()
            .output_dir
    };
    assert_eq!(stored(&pipeline), moved);

    // Sent for again, the job starts in the directory it moved to.
    activate_job(&mut pipeline, id).await;
    assert!(matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    assert_eq!(pipeline.jobs[&id].working_dir, moved);
    assert!(pipeline.moving_jobs.is_empty());
    let dirs = std::fs::read_dir(&new_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(dirs, vec![moved]);
}

// One queued job that never started, its intermediate folder then changed.
async fn unstarted_job_with_a_changed_folder(
    temp: &tempfile::TempDir,
    id: JobId,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, old_root, complete) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    enqueue(&mut pipeline, id.0).await;
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

// A job that never started is let go while its first read moves its working
// directory, and leaves the pipeline's installed jobs before the reply: the
// move is not lost. The stored row and the queue row name the new
// directory, the old one is gone, and nothing else is left under the root.
#[tokio::test]
async fn a_move_heard_after_the_job_was_let_go_is_kept_on_its_row() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;

    pipeline.request_job_activation(id);
    pipeline.release_job_articles(id);
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    // The reply finds the job no longer installed, as one paused out of the
    // window is not.
    let state = pipeline.jobs.remove(&id).unwrap();
    pipeline.handle_job_activated(done);
    assert!(pipeline.moving_jobs.is_empty());

    let stored = pipeline
        .db
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored.parent(), Some(new_root.as_path()));
    assert!(crate::jobs::working_dir::is_job_working_dir(&new_root, &stored, id).unwrap());
    assert!(!old_dir.exists(), "the old folder is left behind");
    let listed = pipeline
        .shared_state
        .queue
        .read(|inner| inner.row(id).unwrap().report.job.output_dir.clone());
    assert_eq!(listed, Some(stored.display().to_string()));
    let dirs = std::fs::read_dir(&new_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(dirs, vec![stored]);
    drop(state);
}

// A job paused while its first read moves its working directory, then
// resumed: it starts in the new directory, which its stored row names, and
// that is the one folder there is.
#[tokio::test]
async fn a_job_paused_while_its_move_is_out_resumes_in_the_new_folder() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());

    pipeline.request_job_activation(id);
    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    pipeline.handle_job_activated(done);
    assert!(pipeline.moving_jobs.is_empty());
    let stored = |pipeline: &Pipeline| {
        pipeline
            .db
            .load_active_job_row(id)
            .unwrap()
            .unwrap()
            .output_dir
    };
    let moved = stored(&pipeline);
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(!old_dir.exists(), "the old folder is left behind");

    handle.resume_job(id).await.unwrap();
    settle(&mut pipeline).await;
    if matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Queued(_)
    ) {
        activate_job(&mut pipeline, id).await;
    }

    assert!(matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    assert_eq!(pipeline.jobs[&id].working_dir, moved);
    assert_eq!(stored(&pipeline), moved);
    let dirs = std::fs::read_dir(&new_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(dirs, vec![moved]);
}

// A job that never started whose working directory is gone from under the
// intermediate folder it is still set to is given one there again as it
// starts; its stored row names it.
#[tokio::test]
async fn an_unstarted_job_whose_folder_is_gone_from_the_same_root_claims_one_again() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, root, _) = new_direct_pipeline(&temp).await;
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    let old_dir = pipeline.jobs[&id].working_dir.clone();
    assert_eq!(old_dir.parent(), Some(root.as_path()));
    std::fs::remove_dir_all(&old_dir).unwrap();

    activate_job(&mut pipeline, id).await;

    assert!(matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    let claimed = pipeline.jobs[&id].working_dir.clone();
    assert!(crate::jobs::working_dir::is_job_working_dir(&root, &claimed, id).unwrap());
    let stored = pipeline
        .db
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored, claimed);
}

// A resume stored for a job that waited for a slot goes with the job when it
// is cancelled: retried under the same id, paused and resumed while it waits
// again, its resume is stored again.
#[tokio::test]
async fn a_cancelled_job_s_stored_resume_does_not_outlive_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    settle(&mut pipeline).await;
    let id = JobId(2);
    assert!(!pipeline.nzb_admitted.contains(&id));

    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    handle.resume_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(pipeline.resumes_stored.contains(&id));

    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.resumes_stored.contains(&id));
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    handle.redownload_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(!pipeline.nzb_admitted.contains(&id));

    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(!pipeline.resumes_stored.contains(&id));
    handle.resume_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(pipeline.resumes_stored.contains(&id));
}

// A job that never started whose working directory is gone, with the folder
// it stood in, is given a new one under the intermediate folder set now as
// it starts, and starts.
#[tokio::test]
async fn an_unstarted_job_whose_folder_is_gone_claims_a_new_one() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    std::fs::remove_dir_all(old_dir.parent().unwrap()).unwrap();

    activate_job(&mut pipeline, id).await;

    assert!(matches!(
        pipeline.jobs[&id].articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    let moved = pipeline.jobs[&id].working_dir.clone();
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(crate::jobs::working_dir::is_job_working_dir(&new_root, &moved, id).unwrap());
    let stored = pipeline
        .db
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored, moved);
}

// A job that left the pipeline and the queue while its move was out leaves
// nothing behind: the directory claimed for it goes when the reply is heard.
#[tokio::test]
async fn a_job_gone_before_its_move_is_heard_leaves_no_folder() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;

    pipeline.request_job_activation(id);
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    let claimed = done.moved_to.clone().unwrap();
    assert!(claimed.exists());
    let state = pipeline.jobs.remove(&id).unwrap();
    pipeline.shared_state.queue.write(|inner| {
        inner.remove_job(id);
        crate::pipeline::queue::Change::changed(())
    });
    pipeline.handle_job_activated(done);

    // The removal runs off the actor: wait for the folder itself.
    while claimed.exists() {
        tokio::task::yield_now().await;
    }
    assert!(!old_dir.exists());
    assert_eq!(std::fs::read_dir(&new_root).unwrap().count(), 0);
    drop(state);
}

// A reorder among jobs out of the window changes no installed job, and is
// stored all the same: a restart before they enter keeps it.
#[tokio::test]
async fn a_reorder_among_parked_jobs_is_stored() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let (moved, past) = (JobId(150), JobId(151));
    assert!(!pipeline.jobs.contains_key(&moved) && !pipeline.jobs.contains_key(&past));
    handle
        .reorder_jobs(vec![(moved, QueueMoveTarget::Offset(1))])
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert!(!pipeline.jobs.contains_key(&moved) && !pipeline.jobs.contains_key(&past));
    pipeline.db.flush_write_queue().await.unwrap();
    let stored = pipeline.db.load_active_job_queue_positions().unwrap();
    assert!(
        stored[&past] < stored[&moved],
        "stored {} before {}",
        stored[&moved],
        stored[&past]
    );
}

// An edit given to a parked job is in its row when the job enters the
// window, even while the write that carries it still waits in the job's
// queue: the read waits for it.
#[tokio::test]
async fn a_parked_job_enters_with_the_edit_it_was_given() {
    use crate::persistence::connection::JOB_WRITE_WAITS;
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    let id = JobId(900);
    assert!(pipeline.window_parked(id));
    // The edit's write waits in the job's queue until the gate opens.
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
    let waits = JOB_WRITE_WAITS.load(Ordering::Relaxed);
    let loaded = std::cell::Cell::new(false);
    let load = async {
        let read = pipeline.parked_loads(vec![id]).read().await;
        loaded.set(true);
        read
    };
    // The gate opens once the read waits on the job's writes, or once it
    // read without waiting.
    let opener = async {
        while JOB_WRITE_WAITS.load(Ordering::Relaxed) == waits && !loaded.get() {
            tokio::task::yield_now().await;
        }
        open.send(()).unwrap();
    };
    let (read, ()) = tokio::join!(load, opener);
    let [(read_id, Ok(Some(request)))] = read.as_slice() else {
        panic!("the parked job reads back");
    };
    assert_eq!(*read_id, id);
    assert_eq!(request.spec.category.as_deref(), Some("fresh"));
}

// A job past the window that took the slot but whose stored NZB cannot be
// read back is never installed. Its cancel still archives it, removes its
// row and frees the slot for the next job.
#[tokio::test]
async fn an_unreadable_parked_job_is_cancelled_and_frees_its_slot() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 1).await;
    let unreadable = JobId(QUEUE_WINDOW as u64 + 1);
    let next = JobId(QUEUE_WINDOW as u64 + 2);
    pipeline.db.flush_write_queue().await.unwrap();
    let datastore = pipeline.db.datastore();
    pipeline
        .db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE job_nzbs SET nzb_zstd = {} WHERE job_id = {}",
                &[
                    SqlArg::Bytes(b"not an nzb".to_vec()),
                    SqlArg::I64(unreadable.0 as i64),
                ],
            )
            .await?;
            SqlRuntime::execute(
                datastore.write_exec(),
                // With no stored outline, its entry reads the NZB.
                "UPDATE active_jobs SET nzb_files = NULL WHERE job_id = {}",
                &[SqlArg::I64(unreadable.0 as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();
    // The window's other jobs paused, the slot job 1 frees goes to the job
    // past the window, whose read back fails.
    let window: Vec<JobId> = (2..=QUEUE_WINDOW as u64).map(JobId).collect();
    for outcome in handle.set_jobs_paused(window, true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    handle.cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(pipeline.nzb_admitted.contains(&unreadable));
    assert!(pipeline.window_unreadable.contains(&unreadable));
    assert!(!pipeline.jobs.contains_key(&unreadable));

    handle.cancel_job(unreadable).await.unwrap();
    pipeline.apply_queue_intents().await;
    // The cancel is taken without the job's NZB: its archive is under way.
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| { inner.job(unreadable).is_some_and(|row| row.report.retiring) })
    );
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(
        handle
            .list_jobs()
            .iter()
            .all(|job| job.job_id != unreadable)
    );
    assert!(!pipeline.nzb_admitted.contains(&unreadable));
    assert!(!pipeline.window_unreadable.contains(&unreadable));
    assert!(pipeline.nzb_admitted.contains(&next));
    assert!(pipeline.jobs.contains_key(&next));
    assert!(
        pipeline
            .db
            .get_job_history(unreadable.0)
            .unwrap()
            .is_some_and(|row| row.status == "cancelled")
    );
}
