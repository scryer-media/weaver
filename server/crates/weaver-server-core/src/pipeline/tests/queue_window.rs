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
