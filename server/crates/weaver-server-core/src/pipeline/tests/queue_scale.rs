// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use super::*;
use crate::jobs::handle::QueueMoveTarget;
use crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON;

const QUEUED: u64 = 200;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 4]);
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

// A pipeline holding `QUEUED` jobs with `slots` of them admitted.
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
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), slots as usize);
    (pipeline, handle)
}

fn row_walks() -> u64 {
    crate::pipeline::queue::ROW_WALKS.load(Ordering::Relaxed)
}

#[tokio::test]
async fn a_job_waiting_for_a_slot_stores_the_reason_the_dispatch_pass_computes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline.shared_state.queue.read(|inner| {
        for row in &inner.jobs {
            let stored = row.report.job.download_wait_reason.as_deref();
            assert_eq!(stored, pipeline.stored_wait_reason(row), "job {}", row.id.0);
            if !pipeline.nzb_admitted.contains(&row.id) {
                assert_eq!(stored, Some(NZB_SLOT_WAIT_REASON), "job {}", row.id.0);
            }
        }
    });
    // With the stored and computed reasons agreeing, a dispatch pass finds
    // nothing to re-report, so the queue does not change.
    pipeline.report_changed_dispatch_holds();
    let revision = pipeline.shared_state.queue.revision();
    pipeline.report_changed_dispatch_holds();
    pipeline.report_changed_dispatch_holds();
    assert_eq!(pipeline.shared_state.queue.revision(), revision);
}

#[tokio::test]
async fn a_turn_with_no_new_intent_walks_no_queue_row() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let settled = row_walks();
    for _ in 0..3 {
        pipeline.apply_queue_intents().await;
    }
    assert_eq!(row_walks(), settled);

    // An intent is acted on by the next turn's walk, and the turn after it
    // walks nothing again.
    handle.pause_job(JobId(QUEUED)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(row_walks() > settled);
    assert_eq!(
        pipeline.jobs[&JobId(QUEUED)].run_state,
        crate::RunState::Paused
    );
    let acted = row_walks();
    pipeline.apply_queue_intents().await;
    assert_eq!(row_walks(), acted);
}

#[tokio::test]
async fn a_batch_pause_is_one_queue_change_and_pauses_every_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let queue = pipeline.shared_state.queue.clone();
    let revision = queue.revision();
    let epoch = queue.read(|inner| inner.intent_epoch());
    let outcomes = handle.set_jobs_paused((1..=QUEUED).map(JobId), true).await;
    assert_eq!(outcomes.len(), QUEUED as usize);
    assert!(outcomes.values().all(Result::is_ok));
    assert_eq!(queue.revision(), revision + 1);
    assert_eq!(queue.read(|inner| inner.intent_epoch()), epoch + 1);

    pipeline.apply_queue_intents().await;
    for id in (1..=QUEUED).map(JobId) {
        assert_eq!(
            pipeline.jobs[&id].run_state,
            crate::RunState::Paused,
            "job {}",
            id.0
        );
    }
    // No paused job waits for a slot.
    queue.read(|inner| {
        for row in &inner.jobs {
            assert_ne!(
                row.report.job.download_wait_reason.as_deref(),
                Some(NZB_SLOT_WAIT_REASON),
                "job {}",
                row.id.0
            );
        }
    });
}

#[tokio::test]
async fn a_reorder_keeps_every_row_where_its_index_says() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    handle
        .reorder_jobs(vec![
            (JobId(QUEUED), QueueMoveTarget::Top),
            (JobId(1), QueueMoveTarget::Bottom),
            (JobId(50), QueueMoveTarget::Offset(-10)),
        ])
        .await
        .unwrap();
    let mut expected: Vec<JobId> = (2..QUEUED).map(JobId).collect();
    expected.insert(0, JobId(QUEUED));
    let fifty = expected.iter().position(|id| *id == JobId(50)).unwrap();
    expected.remove(fifty);
    expected.insert(fifty - 10, JobId(50));
    expected.push(JobId(1));
    pipeline.shared_state.queue.read(|inner| {
        let order: Vec<JobId> = inner.jobs.iter().map(|row| row.id).collect();
        assert_eq!(order, expected);
        for (position, id) in order.iter().enumerate() {
            assert_eq!(inner.job_position(*id), Some(position));
        }
    });
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.job_order, expected);

    // The stored positions sort the rows into the same order, which is the
    // order a restart restores.
    pipeline.db.flush_write_queue().await.unwrap();
    let stored = pipeline.db.load_active_job_queue_positions().unwrap();
    let mut restored: Vec<JobId> = stored.keys().copied().collect();
    restored.sort_by_key(|id| stored[id]);
    assert_eq!(restored, expected);
}

#[tokio::test]
async fn per_job_metrics_cover_the_jobs_holding_a_slot() {
    use crate::settings::PerJobSeries;
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    let mut exported: Vec<JobId> = pipeline
        .shared_state
        .metrics_jobs(PerJobSeries::Active)
        .into_iter()
        .map(|job| job.job_id)
        .collect();
    exported.sort_unstable();
    let mut admitted: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    admitted.sort_unstable();
    assert_eq!(exported, admitted);
    assert_eq!(
        pipeline.shared_state.metrics_jobs(PerJobSeries::All).len(),
        QUEUED as usize
    );
    assert!(
        pipeline
            .shared_state
            .metrics_jobs(PerJobSeries::Off)
            .is_empty()
    );
}

#[tokio::test]
async fn a_restart_restores_a_long_queue_a_batch_a_turn_holding_no_article_lists() {
    use crate::pipeline::completion::finalize::RUNTIME_WRITES;
    use crate::pipeline::orchestrator::RESTORE_BATCH;
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    let expected: HashMap<JobId, Vec<crate::jobs::model::FileSummary>> = pipeline
        .jobs
        .iter()
        .map(|(id, state)| {
            let summaries = match &state.articles {
                crate::jobs::model::JobArticles::Queued(summaries) => summaries.0.clone(),
                _ => state
                    .spec
                    .files
                    .iter()
                    .map(crate::jobs::model::FileSummary::of)
                    .collect(),
            };
            (*id, summaries)
        })
        .collect();
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
    // Recovery holds each job's files, never its article lists.
    for candidate in &recovered.to_restore {
        assert!(
            candidate
                .request
                .spec
                .files
                .iter()
                .all(|file| file.segments.is_empty()),
            "job {}",
            candidate.job_id.0
        );
    }
    let order: Vec<JobId> = recovered.to_restore.iter().map(|c| c.job_id).collect();
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }

    // Each turn installs one batch, first in queue order, and says more wait.
    let writes = RUNTIME_WRITES.load(Ordering::Relaxed);
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs.len(), RESTORE_BATCH);
    assert!(pipeline.restores_waiting);
    assert!(
        order[..RESTORE_BATCH]
            .iter()
            .all(|id| pipeline.jobs.contains_key(id))
    );
    let mut turns = 1;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
        turns += 1;
    }
    assert_eq!(pipeline.jobs.len(), QUEUED as usize);
    assert_eq!(turns, (QUEUED as usize).div_ceil(RESTORE_BATCH));
    // A restore that changes nothing in its row writes nothing back.
    assert_eq!(RUNTIME_WRITES.load(Ordering::Relaxed), writes);
    // Every job waits its turn holding the summaries its lists gave.
    for (id, state) in &pipeline.jobs {
        match &state.articles {
            crate::jobs::model::JobArticles::Queued(summaries) => {
                assert_eq!(summaries.0, expected[id], "job {}", id.0)
            }
            _ => assert!(pipeline.nzb_admitted.contains(id), "job {}", id.0),
        }
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 2);
}
