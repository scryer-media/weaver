// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue far longer than its window. Only the window's jobs are installed;
// every other job is its row and its stored rows.

use std::path::PathBuf;

use weaver_server_base::{events::model::PipelineEvent, jobs::ids::JobId};
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::jobs::{
    handle::QueueMoveTarget, persistence::JobsPersistenceQueries, queries::JobsQueries,
};
use weaver_server_pipeline::pipeline::tests::{
    activate_job, finish_cancel_jobs,
    history_fixture::{
        database, installed_staging_dir, make_staging_dir, release_job_articles, staging_dir_for,
    },
    new_direct_pipeline, new_direct_pipeline_at_roots,
    queue_fixture::{
        apply_intents, cleanup_pending, flush_checkpoint, handle as queue_handle, working_dir,
    },
    retire_pipeline_database,
    script_fixture::enable_queue_script,
    window_fixture::*,
};
use weaver_server_pipeline::{JobStatus, SchedulerError, pipeline::Pipeline};

#[tokio::test]
async fn a_long_queue_installs_its_first_window_and_a_priority_raise_moves_it() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW);
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
            weaver_server_pipeline::jobs::JobUpdate {
                metadata_edits: vec![("priority".into(), Some("high".into()))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW);
    assert!(has_installed(&pipeline, JobId(900)));
    assert!(!has_installed(&pipeline, JobId(QUEUE_WINDOW as u64)));
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
    assert!(holders.iter().all(|id| admitted(&pipeline, *id)));
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
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW + holders.len());
    assert!(holders.iter().all(|id| has_installed(&pipeline, *id)));
    assert!((4..4 + QUEUE_WINDOW as u64).all(|id| has_installed(&pipeline, JobId(id))));

    // With the window paused, the job a freed slot admits is past it: it is
    // installed as it takes the slot.
    let window: Vec<JobId> = (4..4 + QUEUE_WINDOW as u64).map(JobId).collect();
    let next = JobId(4 + QUEUE_WINDOW as u64);
    assert!(!has_installed(&pipeline, next));
    for outcome in handle.set_jobs_paused(window, true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    handle.cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(admitted(&pipeline, next));
    assert!(has_installed(&pipeline, next));
    assert!(!has_installed(&pipeline, JobId(1)));
}

#[tokio::test]
async fn a_restart_installs_only_the_window_and_lists_every_row_in_place() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let recovered = weaver_server_pipeline::operations::recover_server_state(
        database(&pipeline),
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
            .all(|job| job.status == JobStatus::Queued || admitted(&pipeline, job.job_id))
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
    set_progress(&mut pipeline, started, 512);
    checkpoint_job(&mut pipeline, started);
    flush_checkpoint(&mut pipeline, started);
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(has_checkpoint(&pipeline, started));
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
    assert!(paused_ids().all(|id| parked(&pipeline, JobId(id))));
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
    assert!(!parked(&pipeline, resumed));

    // A restart installs none of the paused jobs either.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let recovered = weaver_server_pipeline::operations::recover_server_state(
        database(&pipeline),
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
    assert!(!parked(&pipeline, started));
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
    set_progress(&mut pipeline, started, 512);
    checkpoint_job(&mut pipeline, started);
    flush_checkpoint(&mut pipeline, started);
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(has_checkpoint(&pipeline, started));
    for outcome in handle.set_jobs_paused([started], true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    assert!(installed(&pipeline).contains(&started));

    // A restart restores it from its checkpoint, with nothing in hand.
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let recovered = weaver_server_pipeline::operations::recover_server_state(
        database(&pipeline),
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
        parked(&pipeline, started),
        "the moved job stayed in the window: {:?}",
        progress_and_status(&pipeline, started)
    );

    handle
        .reorder_jobs(vec![(started, QueueMoveTarget::Top)])
        .await
        .unwrap();
    settle(&mut pipeline).await;
    let on_disk = |pipeline: &Pipeline| on_disk(pipeline, started);
    assert!(
        on_disk(&pipeline),
        "the re-entered row states its checkpoint"
    );
    for _ in 0..3 {
        assert!(installed(&pipeline).contains(&started));
        assert!(!parked(&pipeline, started));
        settle(&mut pipeline).await;
    }
    assert_eq!(handle.get_job(started).unwrap().status, JobStatus::Paused);
}

// A reorder among jobs out of the window changes no installed job, and is
// stored all the same: a restart before they enter keeps it.
#[tokio::test]
async fn a_reorder_among_parked_jobs_is_stored() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let (moved, past) = (JobId(150), JobId(151));
    assert!(!has_installed(&pipeline, moved) && !has_installed(&pipeline, past));
    handle
        .reorder_jobs(vec![(moved, QueueMoveTarget::Offset(1))])
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert!(!has_installed(&pipeline, moved) && !has_installed(&pipeline, past));
    database(&pipeline).flush_write_queue().await.unwrap();
    let stored = database(&pipeline)
        .load_active_job_queue_positions()
        .unwrap();
    assert!(
        stored[&past] < stored[&moved],
        "stored {} before {}",
        stored[&moved],
        stored[&past]
    );
}

// A job past the window that took the slot but whose stored NZB cannot be
// read back is never installed. Its cancel still archives it, removes its
// row and frees the slot for the next job.
#[tokio::test]
async fn an_unreadable_parked_job_is_cancelled_and_frees_its_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 1).await;
    let unreadable = JobId(QUEUE_WINDOW as u64 + 1);
    let next = JobId(QUEUE_WINDOW as u64 + 2);
    database(&pipeline).flush_write_queue().await.unwrap();
    corrupt_parked_nzb(&pipeline, unreadable);
    // The window's other jobs paused, the slot job 1 frees goes to the job
    // past the window, whose read back fails.
    let window: Vec<JobId> = (2..=QUEUE_WINDOW as u64).map(JobId).collect();
    for outcome in handle.set_jobs_paused(window, true).await.into_values() {
        outcome.unwrap();
    }
    settle(&mut pipeline).await;
    handle.cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(admitted(&pipeline, unreadable));
    assert!(unreadable_job(&pipeline, unreadable));
    assert!(!has_installed(&pipeline, unreadable));

    handle.cancel_job(unreadable).await.unwrap();
    apply_intents(&mut pipeline).await;
    // The cancel is taken without the job's NZB: its archive is under way.
    assert!(retiring(&pipeline, unreadable));
    finish_cancel_jobs(&mut pipeline).await;
    settle(&mut pipeline).await;
    assert!(
        handle
            .list_jobs()
            .iter()
            .all(|job| job.job_id != unreadable)
    );
    assert!(!admitted(&pipeline, unreadable));
    assert!(!unreadable_job(&pipeline, unreadable));
    assert!(admitted(&pipeline, next));
    assert!(has_installed(&pipeline, next));
    assert!(
        database(&pipeline)
            .get_job_history(unreadable.0)
            .unwrap()
            .is_some_and(|row| row.status == "cancelled")
    );
}

// The intermediate folder changes with two jobs queued, and Weaver restarts
// before either runs on. The job that started keeps its working directory;
// the one that never started works under the new folder as it starts, and
// leaves nothing behind in the old one. Another restart keeps both answers.
#[tokio::test]
async fn an_intermediate_folder_change_moves_only_the_jobs_that_never_started() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, old_root, complete) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    let (started, unstarted) = (JobId(1), JobId(2));
    enqueue(&mut pipeline, started.0).await;
    enqueue(&mut pipeline, unstarted.0).await;
    settle(&mut pipeline).await;
    activate_job(&mut pipeline, started).await;
    set_progress(&mut pipeline, started, 512);
    checkpoint_job(&mut pipeline, started);
    flush_checkpoint(&mut pipeline, started);
    database(&pipeline).flush_write_queue().await.unwrap();
    let working = |pipeline: &Pipeline, id: JobId| working_dir(pipeline, id).clone();
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
    assert_eq!(intermediate_dir(&pipeline), new_root);
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
            let handle = queue_handle(&pipeline);
            let recovered = weaver_server_pipeline::operations::recover_server_state(
                database(&pipeline),
                &data,
                &new_root,
            )
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
        if articles_queued(&pipeline, id) {
            activate_job(&mut pipeline, id).await;
        }
    }
    assert!(articles_active(&pipeline, unstarted));
    assert_eq!(working(&pipeline, started), started_dir);
    assert!(started_dir.exists());
    let moved = working(&pipeline, unstarted);
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(is_job_working_dir(&new_root, &moved, unstarted).unwrap());
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
    let handle = queue_handle(&pipeline);
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    let queued = |pipeline: &Pipeline| articles_queued(pipeline, id);
    assert!(queued(&pipeline));
    let old_dir = working_dir(&pipeline, id).clone();
    assert_eq!(old_dir.parent(), Some(old_root.as_path()));

    let new_root = temp.path().join("moved-intermediate");
    handle
        .update_runtime_paths(temp.path().join("data"), new_root.clone(), complete)
        .await
        .unwrap();
    settle(&mut pipeline).await;
    assert_eq!(intermediate_dir(&pipeline), new_root);

    // The first ask has the move; the job is let go before its reply.
    request_activation(&mut pipeline, id);
    assert!(move_pending(&pipeline, id));
    release_job_articles(&mut pipeline, id);
    assert!(queued(&pipeline));
    // The second ask sends nothing while the move is out.
    request_activation(&mut pipeline, id);
    assert!(queued(&pipeline));
    assert!(awaits_articles(&pipeline, id));

    // The first reply is no longer the job's activation, and its move
    // stands all the same.
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    assert!(queued(&pipeline));
    assert!(no_pending_moves(&pipeline));
    let moved = working_dir(&pipeline, id).clone();
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(!old_dir.exists(), "the old folder is left behind");
    // The stored row named the new directory before the old one went.
    let stored = |pipeline: &Pipeline| {
        database(pipeline)
            .load_active_job_row(id)
            .unwrap()
            .unwrap()
            .output_dir
    };
    assert_eq!(stored(&pipeline), moved);

    // Sent for again, the job starts in the directory it moved to.
    activate_job(&mut pipeline, id).await;
    assert!(articles_active(&pipeline, id));
    assert_eq!(*working_dir(&pipeline, id), moved);
    assert!(no_pending_moves(&pipeline));
    let dirs = std::fs::read_dir(&new_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(dirs, vec![moved]);
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

    request_activation(&mut pipeline, id);
    release_job_articles(&mut pipeline, id);
    let done = ActivationReply::receive(&mut pipeline).await;
    // The reply finds the job no longer installed, as one paused out of the
    // window is not.
    let state = uninstall_job(&mut pipeline, id);
    done.deliver(&mut pipeline);
    assert!(no_pending_moves(&pipeline));

    let stored = database(&pipeline)
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored.parent(), Some(new_root.as_path()));
    assert!(is_job_working_dir(&new_root, &stored, id).unwrap());
    assert!(!old_dir.exists(), "the old folder is left behind");
    let listed = reported_output_dir(&pipeline, id);
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
    let handle = queue_handle(&pipeline);

    request_activation(&mut pipeline, id);
    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    assert!(no_pending_moves(&pipeline));
    let stored = |pipeline: &Pipeline| {
        database(pipeline)
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
    if articles_queued(&pipeline, id) {
        activate_job(&mut pipeline, id).await;
    }

    assert!(articles_active(&pipeline, id));
    assert_eq!(*working_dir(&pipeline, id), moved);
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
    let old_dir = working_dir(&pipeline, id).clone();
    assert_eq!(old_dir.parent(), Some(root.as_path()));
    std::fs::remove_dir_all(&old_dir).unwrap();

    activate_job(&mut pipeline, id).await;

    assert!(articles_active(&pipeline, id));
    let claimed = working_dir(&pipeline, id).clone();
    assert!(is_job_working_dir(&root, &claimed, id).unwrap());
    let stored = database(&pipeline)
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
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    settle(&mut pipeline).await;
    let id = JobId(2);
    assert!(!admitted(&pipeline, id));

    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    handle.resume_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(resume_stored(&pipeline, id));

    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!resume_stored(&pipeline, id));
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    handle.redownload_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(!admitted(&pipeline, id));

    handle.pause_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(!resume_stored(&pipeline, id));
    handle.resume_job(id).await.unwrap();
    settle(&mut pipeline).await;
    assert!(resume_stored(&pipeline, id));
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

    assert!(articles_active(&pipeline, id));
    let moved = working_dir(&pipeline, id).clone();
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(is_job_working_dir(&new_root, &moved, id).unwrap());
    let stored = database(&pipeline)
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored, moved);
}

// A job deleted, by the real command, while its move was out leaves nothing
// behind: neither the directory it had nor the one claimed for it.
#[tokio::test]
async fn a_job_deleted_while_its_move_is_out_leaves_no_folder() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();

    request_activation(&mut pipeline, id);
    handle.cancel_job_and_delete_history(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    assert!(no_pending_moves(&pipeline));

    // The removals run off the actor: wait for the folders themselves.
    while old_dir.exists()
        || std::fs::read_dir(&new_root).is_ok_and(|mut entries| entries.next().is_some())
    {
        tokio::task::yield_now().await;
    }
    assert!(rows(&pipeline) == 0);
}

// A job cancelled while its move was out, and retried under the same id
// before the move is heard: the retry has one folder, the one its row
// names, and nothing is left of the first run.
#[tokio::test]
async fn a_job_cancelled_while_its_move_is_out_and_retried_has_one_folder() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();

    request_activation(&mut pipeline, id);
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    // The retry waits for the move to be heard.
    let retried = tokio::spawn({
        let handle = handle.clone();
        async move { handle.redownload_job(id).await }
    });
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    retried.await.unwrap().unwrap();
    settle(&mut pipeline).await;

    let retry = working_dir(&pipeline, id).clone();
    assert_eq!(retry.parent(), Some(new_root.as_path()));
    assert!(is_job_working_dir(&new_root, &retry, id).unwrap());
    let stored = database(&pipeline)
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored, retry);
    // The removals run off the actor: wait for the folders themselves.
    while old_dir.exists() || std::fs::read_dir(&new_root).unwrap().count() != 1 {
        tokio::task::yield_now().await;
    }
    assert!(retry.exists());
}

#[tokio::test]
async fn each_add_raises_its_arrival_script_once() {
    const ADDS: u64 = 500;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    let raised = added_script_raises();
    for id in 1..=ADDS {
        enqueue(&mut pipeline, id).await;
    }
    settle(&mut pipeline).await;
    assert_eq!(added_script_raises() - raised, ADDS);
    assert_eq!(rows(&pipeline), ADDS as usize);
    // Jobs entering and leaving the window raise nothing again.
    let mut ids: Vec<JobId> = (1..=ADDS).map(JobId).collect();
    ids.reverse();
    let handle = queue_handle(&pipeline);
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
    assert_eq!(added_script_raises() - raised, ADDS);
}

// The retried job's one folder, under `new_root`, which its stored row
// names; nothing else is left there or in `old_dir`.
async fn the_retry_has_one_folder(
    pipeline: &Pipeline,
    id: JobId,
    old_dir: &std::path::Path,
    new_root: &std::path::Path,
) {
    let retry = working_dir(pipeline, id).clone();
    assert_eq!(retry.parent(), Some(new_root));
    assert!(is_job_working_dir(new_root, &retry, id).unwrap());
    let stored = database(pipeline)
        .load_active_job_row(id)
        .unwrap()
        .unwrap()
        .output_dir;
    assert_eq!(stored, retry);
    // Every removal of the first run's folders is done: none is out.
    wait_for_move_cleanup(pipeline, id).await;
    assert!(!old_dir.exists(), "the first run's folder is left");
    let dirs = std::fs::read_dir(new_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(dirs, vec![retry]);
}

// A job cancelled while its move was out, and retried while the move's last
// step on disk is held: the retry waits for that step, and only then clears
// the job's folders and makes its own. Gone on first, it would look at a
// folder the move is removing, or make one beside the move's.
#[tokio::test]
async fn a_retry_waits_for_the_move_that_is_out() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let gate = MoveGate::hold_move(&mut pipeline);
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();

    request_activation(&mut pipeline, id);
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    gate.reached().await;
    let retry = tokio::spawn({
        let handle = handle.clone();
        async move { handle.redownload_job(id).await }
    });
    assert_eq!(
        retry_waits_or_clears(&pipeline).await,
        (1, 0),
        "the retry went on while the move was out"
    );

    gate.open();
    retry.await.unwrap().unwrap();
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    settle(&mut pipeline).await;
    the_retry_has_one_folder(&pipeline, id, &old_dir, &new_root).await;
}

// A move heard after its job was cancelled removes the folder it claimed,
// which nothing took. A retry that comes while that removal is held waits
// for it: gone on first, it would make its folder under the same name, for
// the removal to take.
#[tokio::test]
async fn a_retry_waits_for_the_removal_of_the_folder_a_move_claimed() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, old_dir, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let gate = MoveGate::hold_removal(&mut pipeline);
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();

    request_activation(&mut pipeline, id);
    let done = ActivationReply::receive(&mut pipeline).await;
    assert!(done.moved_to().is_some(), "the read moved the job");
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    done.deliver(&mut pipeline);
    gate.reached().await;
    let retry = tokio::spawn({
        let handle = handle.clone();
        async move { handle.redownload_job(id).await }
    });
    assert_eq!(
        retry_waits_or_clears(&pipeline).await,
        (1, 0),
        "the retry went on while the removal was out"
    );

    gate.open();
    retry.await.unwrap().unwrap();
    settle(&mut pipeline).await;
    the_retry_has_one_folder(&pipeline, id, &old_dir, &new_root).await;
}

// A job that never started is cancelled while its move holds a new folder
// its row will not take, and is retried at once, after the cancel's own
// cleanup. Returns the pipeline, the move's seams, the retry, and the new
// intermediate root. The retry is spawned once the move has claimed.
async fn retried_while_the_move_holds_a_folder(
    temp: &tempfile::TempDir,
    id: JobId,
    listing: bool,
) -> (
    Pipeline,
    MoveSeams,
    tokio::task::JoinHandle<Result<(), SchedulerError>>,
    PathBuf,
) {
    // Named as its NZB file is, so the retry claims the name the move did.
    let name = format!("window-{}", id.0);
    let (mut pipeline, _, new_root) =
        unstarted_job_named_with_a_changed_folder(temp, id, &name).await;
    let seams = MoveSeams::hold_move(&new_root, listing);
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    request_activation(&mut pipeline, id);
    // The move has claimed, and waits at its row write, before the cancel's
    // cleanup can take the folder it moves from.
    while !seams.claimed() {
        tokio::task::yield_now().await;
    }
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    // The cancel's archive has committed: the move's row write takes nothing.
    // The cancel's cleanup of its folders waits for the move, so only the
    // move holds the retry back.
    seams.open_row();
    let retried = tokio::spawn(async move { handle.redownload_job(id).await });
    (pipeline, seams, retried, new_root)
}

// Ordering 1: the retry lists the move's folder, the move gives it up, and
// the retry's removal finds it gone. The retry waits for the move instead,
// and goes through.
#[tokio::test]
async fn a_retry_waits_for_the_move_to_give_its_folder_up() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, seams, retried, _) =
        retried_while_the_move_holds_a_folder(&temp, id, true).await;
    while !seams.listed() && !seams.cleanup_waited() {
        tokio::task::yield_now().await;
    }
    seams.open_abandon();
    while !seams.abandoned() {
        tokio::task::yield_now().await;
    }
    seams.open_listing();
    let done = ActivationReply::receive(&mut pipeline).await;
    assert!(done.moved_to().is_none(), "the row took nothing");
    done.deliver(&mut pipeline);
    retried.await.unwrap().unwrap();
    seams.disarm();
}

// Ordering 2: the retry clears the move's folder and claims the same name,
// then the move gives up what is now the retry's folder. The retry waits
// for the move instead, and keeps the folder its row names.
#[tokio::test]
async fn a_retry_keeps_its_folder_when_the_move_gives_its_own_up() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, seams, retried, new_root) =
        retried_while_the_move_holds_a_folder(&temp, id, false).await;
    while !retried.is_finished() && !seams.cleanup_waited() {
        tokio::task::yield_now().await;
    }
    seams.open_abandon();
    while !seams.abandoned() {
        tokio::task::yield_now().await;
    }
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    retried.await.unwrap().unwrap();
    settle(&mut pipeline).await;
    seams.disarm();

    let retry = working_dir(&pipeline, id).clone();
    assert_eq!(retry.parent(), Some(new_root.as_path()));
    assert!(retry.exists(), "the retry's folder is gone");
    assert!(is_job_working_dir(&new_root, &retry, id).unwrap());
}

// A job's read makes it a staging directory on a destination on another
// device, and the job is cancelled before the reply is heard: the reply
// removes that directory, and the cancel's hold lasts until it is gone.
#[tokio::test]
async fn a_staging_folder_made_for_a_job_cancelled_meanwhile_goes() {
    let temp = tempfile::tempdir().unwrap();
    // The roots new_direct_pipeline uses, on two devices before it opens.
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("intermediate"), 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("complete"), 2);
    let (mut pipeline, _, complete) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    assert!(articles_queued(&pipeline, id));

    request_activation(&mut pipeline, id);
    let done = ActivationReply::receive(&mut pipeline).await;
    let staging = staging_dir_for(&complete, id);
    assert!(staging.exists(), "the read made no staging directory");
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    done.deliver(&mut pipeline);

    while cleanup_pending(&pipeline, id) {
        tokio::task::yield_now().await;
    }
    assert!(!staging.exists(), "the staging directory is left behind");
}

// A job that started, so no move is out, is cancelled before the reply that
// made it a staging directory is heard. While that directory's removal is
// held, a restart of the id waits; once the removal ends the restart goes
// through, and the staging directory the retry makes is there.
#[tokio::test]
async fn a_restart_waits_for_the_staging_removal_of_a_job_that_left() {
    let temp = tempfile::tempdir().unwrap();
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("intermediate"), 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("complete"), 2);
    let (mut pipeline, _, complete) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    // Started: its read moves nothing, so no move holds a cancel back.
    mark_started(&mut pipeline, id);

    request_activation(&mut pipeline, id);
    assert!(no_pending_moves(&pipeline));
    let done = ActivationReply::receive(&mut pipeline).await;
    let staging = staging_dir_for(&complete, id);
    assert!(staging.exists(), "the read made no staging directory");
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    let seams = MoveSeams::hold_staging_removal(&complete);
    // The reply's removal is held at its gate.
    done.deliver(&mut pipeline);
    while cancel_folders_held(&pipeline, id) {
        tokio::task::yield_now().await;
    }
    let retried = tokio::spawn({
        let handle = handle.clone();
        async move { handle.redownload_job(id).await }
    });
    while !retried.is_finished() && !seams.cleanup_waited() {
        tokio::task::yield_now().await;
    }
    // A retry that did not wait has made its staging directory before the
    // held removal runs.
    let waited = !retried.is_finished();
    let retried = if waited {
        Some(retried)
    } else {
        retried.await.unwrap().unwrap();
        settle(&mut pipeline).await;
        activate_job(&mut pipeline, id).await;
        None
    };
    seams.open_staging_removal();
    while !seams.staging_removed() {
        tokio::task::yield_now().await;
    }
    if let Some(retried) = retried {
        retried.await.unwrap().unwrap();
        settle(&mut pipeline).await;
        activate_job(&mut pipeline, id).await;
    }
    seams.disarm();

    assert_eq!(
        installed_staging_dir(&pipeline, id).as_ref(),
        Some(&staging)
    );
    assert!(staging.exists(), "the retry's staging directory is gone");
    assert!(waited, "the restart did not wait for the staging removal");
}

// The cancel clears the staging directory the job's state never named. One
// the disowned reply names that holds files by then is kept.
#[tokio::test]
async fn a_left_job_reply_keeps_a_staging_folder_holding_files() {
    let temp = tempfile::tempdir().unwrap();
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("intermediate"), 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&temp.path().join("complete"), 2);
    let (mut pipeline, _, complete) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    let id = JobId(1);
    enqueue(&mut pipeline, id.0).await;
    settle(&mut pipeline).await;
    request_activation(&mut pipeline, id);
    let done = ActivationReply::receive(&mut pipeline).await;
    let staging = staging_dir_for(&complete, id);
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while cancel_folders_held(&pipeline, id) {
        tokio::task::yield_now().await;
    }
    assert!(!staging.exists(), "the cancel left the staging directory");

    make_staging_dir(&staging, id).unwrap();
    std::fs::write(staging.join("payload"), b"keep").unwrap();
    done.deliver(&mut pipeline);
    while cleanup_pending(&pipeline, id) {
        tokio::task::yield_now().await;
    }
    assert_eq!(std::fs::read(staging.join("payload")).unwrap(), b"keep");
}

// A job cancelled while its move was out, its id then being admitted again
// or held by a history operation when the move is heard: whoever works with
// the id may have taken the moved directory as its own, so the reply keeps
// it. With no one working with the id, the same reply lets it go.
#[tokio::test]
async fn a_move_heard_while_its_id_is_worked_with_removes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let id = JobId(1);
    let (mut pipeline, _, new_root) = unstarted_job_with_a_changed_folder(&temp, id).await;
    let handle = queue_handle(&pipeline);
    let mut events = handle.subscribe_events();

    // The move is made and waits to be heard; the job is cancelled first.
    request_activation(&mut pipeline, id);
    let done = ActivationReply::receive(&mut pipeline).await;
    let moved = done
        .moved_to()
        .expect("the read moved the job")
        .to_path_buf();
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert_eq!(moved.parent(), Some(new_root.as_path()));
    assert!(!has_queue_row(&pipeline, id));

    for held_by_history in [false, true] {
        hold_move_owner(&pipeline, id, held_by_history);
        assert!(done.take_moved_dir(&mut pipeline));
        release_move_owner(&pipeline, id);
    }
    assert!(!done.take_moved_dir(&mut pipeline));

    // Heard with no one working with the id, the directory goes, off the
    // actor: wait for the folder itself.
    done.deliver(&mut pipeline);
    while moved.exists() {
        tokio::task::yield_now().await;
    }
}

// An edit given to a parked job is in its row when the job enters the
// window, even while the write that carries it still waits in the job's
// queue: the read waits for it.
#[tokio::test]
async fn a_parked_job_enters_with_the_edit_it_was_given() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    let id = JobId(900);
    assert!(parked(&pipeline, id));
    // The edit's write waits in the job's queue until the gate opens.
    let open = gate_parked_category_edit(&pipeline, id);
    let waits = parked_write_waits();
    let loaded = std::cell::Cell::new(false);
    let load = async {
        let read = read_parked(&pipeline, id).await;
        loaded.set(true);
        read
    };
    // The gate opens once the read waits on the job's writes, or once it
    // read without waiting.
    let opener = async {
        while parked_write_waits() == waits && !loaded.get() {
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
