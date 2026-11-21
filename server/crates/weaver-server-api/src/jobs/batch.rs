// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Queue batches: one request names many jobs, by id or by a selection, and
//! the queue takes them as one change.

use std::collections::{BTreeMap, BTreeSet};

use async_graphql::ErrorExtensions;
use weaver_server_core::jobs::handle::CancellationOrigin;
use weaver_server_core::tags::TagSnapshot;
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

/// The jobs a target names: its distinct ids as given, or every queued job
/// its selection matches, in queue order, provided the selection still
/// matches the count the caller confirmed.
pub(crate) fn resolve_target(
    state: &SharedPipelineState,
    target: &QueueBatchTarget,
    tags: Option<&TagSnapshot>,
) -> async_graphql::Result<Vec<JobId>> {
    match (&target.ids, &target.selection) {
        (Some(ids), None) => {
            if target.expected_matched.is_some() {
                return Err(graphql_error(
                    "INVALID_INPUT",
                    "expectedMatched goes with a selection, not with ids",
                ));
            }
            let mut seen = BTreeSet::new();
            let ids: Vec<JobId> = ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id))
                .map(JobId)
                .collect();
            if ids.len() > MAX_QUEUE_BATCH_IDS {
                return Err(graphql_error(
                    "BATCH_TOO_LARGE",
                    format!(
                        "a batch names at most {MAX_QUEUE_BATCH_IDS} ids; name more by a selection"
                    ),
                ));
            }
            Ok(ids)
        }
        (None, Some(input)) => {
            let selection = QueueSelection::of_input(input).tagged(tags);
            if selection.is_unfiltered() && input.all != Some(true) {
                return Err(graphql_error(
                    "INVALID_INPUT",
                    "a selection with no filter names the whole queue only with all: true",
                ));
            }
            let Some(expected) = target.expected_matched else {
                return Err(graphql_error(
                    "INVALID_INPUT",
                    "a selection carries expectedMatched, the count the caller confirmed",
                ));
            };
            let walk = QueueWalk::read(state, selection.needs(None));
            let ids = selection.ids(&walk);
            let matched = u32::try_from(ids.len()).unwrap_or(u32::MAX);
            if matched != expected {
                return Err(async_graphql::Error::new(format!(
                    "the selection now matches {matched} jobs, not {expected}; confirm again"
                ))
                .extend_with(|_, extensions| {
                    extensions.set("code", "CONFLICT");
                    extensions.set("matched", matched);
                }));
            }
            Ok(ids)
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
        QueueBatchOp::Pause => handle.set_jobs_paused(ids, true).await,
        QueueBatchOp::Resume => handle.set_jobs_paused(ids, false).await,
        QueueBatchOp::Update(update) => {
            handle
                .update_jobs(ids.into_iter().map(|id| (id, update.clone())))
                .await
        }
    }
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
