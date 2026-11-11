// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The queue's one dispatch order against what each consumer computed for
// itself before it took its order from it, over seeded queues of mixed
// priorities with paused, admitted, cancelled, retiring and edited rows.

use super::*;
use crate::pipeline::queue::Change;
use crate::pipeline::queue::order::{DispatchKey, metadata_priority};

const QUEUED: u64 = 150;
const SLOTS: u32 = 4;

fn priority_of(id: u64) -> &'static str {
    match id % 3 {
        0 => "high",
        1 => "normal",
        _ => "low",
    }
}

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let mut spec = segmented_job_spec(&format!("Order NZB {id}"), "payload.bin", &[512; 4]);
    spec.metadata = vec![("priority".to_string(), priority_of(id).to_string())];
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("order-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

async fn settle(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

fn priority_edit(priority: &str) -> crate::jobs::JobUpdate {
    crate::jobs::JobUpdate {
        metadata_edits: vec![("priority".into(), Some(priority.into()))],
        ..Default::default()
    }
}

// A queue longer than the window, every priority, slots taken, two jobs
// paused, a priority edit applied, and, written after the last turn so the
// pipeline has not heard them: two priority edits, a cancel and a retiring
// row.
async fn seeded() -> (tempfile::TempDir, Pipeline, SchedulerHandle) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    // A ring deep enough for the pending edits below to reach it.
    pipeline.next_up.depth = 10;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(SLOTS).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    settle(&mut pipeline).await;
    for id in [JobId(15), JobId(16)] {
        handle.pause_job(id).await.unwrap();
    }
    handle.update_job(JobId(40), priority_edit("high")).await.unwrap();
    settle(&mut pipeline).await;
    let admitted: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    for id in admitted {
        activate_job(&mut pipeline, id).await;
    }
    handle.update_job(JobId(121), priority_edit("high")).await.unwrap();
    handle.update_job(JobId(18), priority_edit("low")).await.unwrap();
    handle.cancel_job(JobId(21)).await.unwrap();
    pipeline.shared_state.queue.write(|inner| {
        inner.job_mut(JobId(24)).unwrap().report.retiring = true;
        Change::changed(())
    });
    (temp, pipeline, handle)
}

// What the window, the admission pass and the ring each read as a row's
// priority before the shared order: with a pending edit in force.
fn former_row_priority(
    row: &crate::pipeline::queue::JobRow,
    globals: &crate::pipeline::queue::Globals,
) -> u8 {
    if row.intents.update.is_some() {
        metadata_priority(&row.info(globals).metadata)
    } else {
        metadata_priority(&row.report.job.metadata)
    }
}

fn former_window(pipeline: &Pipeline) -> HashSet<JobId> {
    pipeline.shared_state.queue.read(|inner| {
        let globals = &inner.globals;
        let size = crate::pipeline::queue_window::window_size(
            globals
                .max_concurrent_nzbs
                .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                .get() as usize,
        );
        let mut ranked: Vec<(u8, usize, JobId)> = inner
            .jobs
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.intents.cancelled.is_none()
                    && !row.report.retiring
                    && !pipeline.blocked_restores.contains_key(&row.id)
                    && !pipeline.window_unreadable.contains(&row.id)
            })
            .map(|(position, row)| (former_row_priority(row, globals), position, row.id))
            .collect();
        if ranked.len() > size {
            ranked.select_nth_unstable(size - 1);
            ranked.truncate(size);
        }
        ranked.into_iter().map(|(_, _, id)| id).collect()
    })
}

// The admission pass's candidate order, over every row.
fn former_admission_order(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.shared_state.queue.read(|inner| {
        let mut rows: Vec<(u8, usize, JobId)> = inner
            .jobs
            .iter()
            .enumerate()
            .map(|(position, row)| (former_row_priority(row, &inner.globals), position, row.id))
            .collect();
        rows.sort_unstable();
        rows.into_iter().map(|(_, _, id)| id).collect()
    })
}

// The ring read the reported priority alone, a pending edit not in force.
fn former_next_up(pipeline: &Pipeline) -> Vec<JobId> {
    let depth = pipeline.next_up.depth;
    pipeline.shared_state.queue.read(|inner| {
        let mut waiting: Vec<(u8, usize, JobId)> = inner
            .jobs
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.intents.cancelled.is_none()
                    && !row.report.retiring
                    && !pipeline.nzb_admitted.contains(&row.id)
            })
            .filter(|(_, row)| {
                pipeline.jobs.get(&row.id).is_some_and(|state| {
                    matches!(state.articles, crate::jobs::model::JobArticles::Queued(_))
                        && state.status == JobStatus::Queued
                        && state.run_state != crate::jobs::model::RunState::Paused
                })
            })
            .map(|(position, row)| {
                (
                    metadata_priority(&row.report.job.metadata),
                    position,
                    row.id,
                )
            })
            .collect();
        if waiting.len() > depth {
            waiting.select_nth_unstable(depth - 1);
            waiting.truncate(depth);
        }
        waiting.into_iter().map(|(_, _, id)| id).collect()
    })
}

fn former_eligible(pipeline: &Pipeline) -> Vec<JobId> {
    let candidates: Vec<(JobId, usize)> = pipeline.shared_state.queue.read(|inner| {
        pipeline
            .nzb_admitted
            .iter()
            .filter_map(|job_id| {
                let position = match inner.job_position(*job_id) {
                    Some(position) => {
                        let row = &inner.jobs[position];
                        if row.intents.cancelled.is_some() || row.intents.paused == Some(true) {
                            return None;
                        }
                        position
                    }
                    None => {
                        inner.jobs.len()
                            + pipeline.job_order.iter().position(|id| id == job_id)?
                    }
                };
                Some((*job_id, position))
            })
            .collect()
    });
    let mut eligible = Vec::new();
    for (job_id, position) in candidates {
        let Some(state) = pipeline.jobs.get(&job_id) else {
            continue;
        };
        // The scheduler's `has_dispatchable_work`.
        let dispatchable = state.articles.has_queued_download_work()
            && matches!(state.status, JobStatus::Queued | JobStatus::Downloading);
        if !dispatchable || pipeline.held_for_added_scripts(job_id) {
            continue;
        }
        let priority = metadata_priority(&state.spec.metadata);
        eligible.push((priority, position, job_id));
    }
    eligible.sort_unstable();
    eligible.into_iter().map(|(_, _, job_id)| job_id).collect()
}

fn canonical_admission_order(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.shared_state.queue.read(|inner| {
        let mut keys = inner.dispatch_keys(|_| true);
        keys.sort_unstable();
        keys.into_iter().map(DispatchKey::job_id).collect()
    })
}

fn sorted(mut ids: Vec<JobId>) -> Vec<JobId> {
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn every_consumer_ranks_as_it_did_before_the_shared_order() {
    let (_temp, pipeline, _handle) = seeded().await;
    assert_eq!(pipeline.nzb_admitted.len(), SLOTS as usize);

    let window = pipeline
        .shared_state
        .queue
        .read(|inner| pipeline.queue_window_ranks(inner));
    assert_eq!(window, former_window(&pipeline));
    assert!(window.contains(&JobId(121)), "a pending raise is in force");

    assert_eq!(
        canonical_admission_order(&pipeline),
        former_admission_order(&pipeline)
    );

    let eligible = pipeline.download_scheduler_eligible_jobs();
    assert!(!eligible.is_empty());
    assert_eq!(eligible, former_eligible(&pipeline));

    // The ring now reads a pending edit in force, as the window and the
    // admission pass do. With the edits applied the two agree.
    let ring = sorted(pipeline.next_up_jobs());
    let former = sorted(former_next_up(&pipeline));
    assert_ne!(ring, former, "the pending edits move the ring");
}

#[tokio::test]
async fn the_ring_ranks_as_before_once_every_edit_is_applied() {
    let (_temp, mut pipeline, _handle) = seeded().await;
    settle(&mut pipeline).await;
    let pending = pipeline
        .shared_state
        .queue
        .read(|inner| inner.jobs.iter().any(|row| row.intents.update.is_some()));
    assert!(!pending);
    assert_eq!(
        sorted(pipeline.next_up_jobs()),
        sorted(former_next_up(&pipeline))
    );
    let window = pipeline
        .shared_state
        .queue
        .read(|inner| pipeline.queue_window_ranks(inner));
    assert_eq!(window, former_window(&pipeline));
    assert_eq!(
        canonical_admission_order(&pipeline),
        former_admission_order(&pipeline)
    );
    assert_eq!(
        pipeline.download_scheduler_eligible_jobs(),
        former_eligible(&pipeline)
    );
}

// Moves, priority changes and pauses reach every consumer through the one
// order: after each, the window, the admission order and the ring are each
// the shared order filtered by what that consumer considers.
#[tokio::test]
async fn operator_moves_reach_every_consumer_the_same_way() {
    use crate::jobs::handle::QueueMoveTarget;
    let (_temp, mut pipeline, _handle) = seeded().await;
    settle(&mut pipeline).await;
    type Step = Box<dyn Fn(&crate::pipeline::queue::Queue)>;
    let steps: Vec<Step> = vec![
        Box::new(|q| q.reorder_jobs(vec![(JobId(140), QueueMoveTarget::Top)]).unwrap()),
        Box::new(|q| q.reorder_jobs(vec![(JobId(30), QueueMoveTarget::Bottom)]).unwrap()),
        Box::new(|q| q.reorder_jobs(vec![(JobId(33), QueueMoveTarget::Offset(-20))]).unwrap()),
        Box::new(|q| q.update_job(JobId(149), priority_edit("high")).unwrap()),
        Box::new(|q| q.set_jobs_paused([JobId(42)], true).into_values().for_each(|o| o.unwrap())),
    ];
    let queue = pipeline.shared_state.queue.clone();
    for step in steps {
        step(&queue);
        settle(&mut pipeline).await;
        let order = canonical_admission_order(&pipeline);
        assert_eq!(order, former_admission_order(&pipeline));
        let window = pipeline
            .shared_state
            .queue
            .read(|inner| pipeline.queue_window_ranks(inner));
        assert_eq!(window, former_window(&pipeline));
        // The ring is the first jobs of the shared order that wait their
        // turn installed.
        let waiting: Vec<JobId> = order
            .iter()
            .copied()
            .filter(|id| {
                !pipeline.nzb_admitted.contains(id)
                    && pipeline.jobs.get(id).is_some_and(|state| {
                        matches!(state.articles, crate::jobs::model::JobArticles::Queued(_))
                            && state.status == JobStatus::Queued
                            && state.run_state != crate::jobs::model::RunState::Paused
                    })
                    && pipeline.shared_state.queue.read(|inner| {
                        inner
                            .job(*id)
                            .is_some_and(|row| row.intents.cancelled.is_none() && !row.report.retiring)
                    })
            })
            .take(pipeline.next_up.depth)
            .collect();
        assert_eq!(sorted(pipeline.next_up_jobs()), sorted(waiting));
    }
}
