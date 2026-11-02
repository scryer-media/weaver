// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Queue batches: one request names many jobs, by id or by a selection, and
//! the queue takes them as one change.

use std::collections::{BTreeMap, BTreeSet};

use weaver_server_core::jobs::handle::CancellationOrigin;
use weaver_server_core::{JobId, JobUpdate, SchedulerError, SchedulerHandle, SharedPipelineState};

use super::paging::{QueueSelection, QueueWalk};
use super::types::{QueueBatchRefusal, QueueBatchResult, QueueBatchTarget};
use crate::auth::graphql_error;

/// The most ids one batch may name. A larger set is named by a selection.
pub const MAX_QUEUE_BATCH_IDS: usize = 1_000;

/// What a batch does to each job it names.
pub(crate) enum QueueBatchOp {
    Pause,
    Resume,
    Cancel,
    Update(JobUpdate),
}

/// Each job's answer, by id.
pub(crate) type BatchOutcome = BTreeMap<JobId, Result<(), SchedulerError>>;

/// The jobs a target names: its ids as given, or every queued job its
/// selection matches, in queue order.
pub(crate) fn resolve_target(
    state: &SharedPipelineState,
    target: &QueueBatchTarget,
) -> async_graphql::Result<Vec<JobId>> {
    match (&target.ids, &target.selection) {
        (Some(ids), None) => {
            if ids.len() > MAX_QUEUE_BATCH_IDS {
                return Err(graphql_error(
                    "BATCH_TOO_LARGE",
                    format!(
                        "a batch names at most {MAX_QUEUE_BATCH_IDS} ids; name more by a selection"
                    ),
                ));
            }
            let mut seen = BTreeSet::new();
            Ok(ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id))
                .map(JobId)
                .collect())
        }
        (None, Some(selection)) => {
            let selection = QueueSelection::of_input(selection);
            let walk = QueueWalk::read(state, selection.needs(None));
            Ok(selection.ids(&walk))
        }
        _ => Err(graphql_error(
            "INVALID_INPUT",
            "a batch names its jobs by ids or by a selection, not both",
        )),
    }
}

/// Applies `op` to every job in `ids` as one queue change. A job that is
/// missing or refuses never holds back the rest.
pub(crate) async fn apply(
    handle: &SchedulerHandle,
    ids: Vec<JobId>,
    op: QueueBatchOp,
) -> BatchOutcome {
    match op {
        QueueBatchOp::Cancel => {
            match handle
                .cancel_jobs(ids.iter().copied().collect(), CancellationOrigin::User)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => ids
                    .into_iter()
                    .map(|id| (id, Err(SchedulerError::Internal(error.to_string()))))
                    .collect(),
            }
        }
        QueueBatchOp::Pause => set_paused(handle, ids, true).await,
        QueueBatchOp::Resume => set_paused(handle, ids, false).await,
        QueueBatchOp::Update(update) => {
            update_jobs(handle, ids.into_iter().map(|id| (id, update.clone()))).await
        }
    }
}

// The core takes a batch of pauses or updates as one queue change once its
// batch entry points land; until then each job is one intent.
async fn set_paused(handle: &SchedulerHandle, ids: Vec<JobId>, paused: bool) -> BatchOutcome {
    let mut outcome = BatchOutcome::new();
    for id in ids {
        let result = if paused {
            handle.pause_job(id).await
        } else {
            handle.resume_job(id).await
        };
        outcome.insert(id, result);
    }
    outcome
}

async fn update_jobs(
    handle: &SchedulerHandle,
    updates: impl IntoIterator<Item = (JobId, JobUpdate)>,
) -> BatchOutcome {
    let mut outcome = BatchOutcome::new();
    for (id, update) in updates {
        outcome.insert(id, handle.update_job(id, update).await);
    }
    outcome
}

/// The answer a batch returns: how many it named, how many took, and why
/// each other one did not.
pub(crate) fn batch_result(outcome: BatchOutcome) -> QueueBatchResult {
    let matched = u32::try_from(outcome.len()).unwrap_or(u32::MAX);
    let refused: Vec<QueueBatchRefusal> = outcome
        .into_iter()
        .filter_map(|(id, result)| {
            result.err().map(|error| QueueBatchRefusal {
                id: id.0,
                code: match error {
                    SchedulerError::JobNotFound(_) => "NOT_FOUND",
                    SchedulerError::Conflict(_) => "CONFLICT",
                    _ => "INTERNAL",
                }
                .to_string(),
                message: error.to_string(),
            })
        })
        .collect();
    QueueBatchResult {
        matched,
        applied: matched.saturating_sub(u32::try_from(refused.len()).unwrap_or(u32::MAX)),
        refused,
    }
}
