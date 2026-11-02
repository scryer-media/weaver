// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use super::{Change, Queue};
use crate::jobs::handle::{
    CancelJobsOutcome, CancellationOrigin, QueueMoveTarget, splice_job_order,
};
use crate::jobs::model::{DownloadState, FieldUpdate, JobStatus, PostState};
use crate::jobs::{JobId, JobUpdate, SchedulerError};

impl Queue {
    pub fn set_job_paused(&self, job_id: JobId, paused: bool) -> Result<(), SchedulerError> {
        self.set_jobs_paused([job_id], paused)
            .remove(&job_id)
            .unwrap_or(Err(SchedulerError::JobNotFound(job_id)))
    }

    // Pause or resume every job named, as one intent: one revision and one
    // wake for the whole batch, each job answered on its own.
    pub fn set_jobs_paused(
        &self,
        ids: impl IntoIterator<Item = JobId>,
        paused: bool,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
        self.write_intent(|inner| {
            let mut outcomes = BTreeMap::new();
            let mut changed = false;
            for job_id in ids {
                let outcome = set_row_paused(inner, job_id, paused);
                changed |= outcome.is_ok();
                outcomes.insert(job_id, outcome);
            }
            if changed {
                Change::changed(outcomes)
            } else {
                Change::unchanged(outcomes)
            }
        })
    }

    pub fn cancel_jobs(
        &self,
        ids: BTreeSet<JobId>,
        origin: CancellationOrigin,
    ) -> CancelJobsOutcome {
        self.cancel_jobs_with_history_policy(ids, origin, false)
    }

    pub(crate) fn cancel_jobs_with_history_policy(
        &self,
        ids: BTreeSet<JobId>,
        origin: CancellationOrigin,
        delete_history: bool,
    ) -> CancelJobsOutcome {
        self.write_intent(|inner| {
            let mut result = CancelJobsOutcome::new();
            let mut changed = false;
            for id in ids {
                let outcome = match inner.job_mut(id) {
                    None => Err(SchedulerError::JobNotFound(id)),
                    Some(row) => {
                        let job = &row.report.job;
                        if row.report.retiring {
                            Err(SchedulerError::Conflict("job is being archived".into()))
                        } else if row.report.moving || job.status == JobStatus::Moving {
                            Err(SchedulerError::Conflict(
                                "cancel is not supported while the final move is running".into(),
                            ))
                        } else if !origin.is_user()
                            && (!matches!(
                                job.download_state,
                                DownloadState::Queued | DownloadState::Downloading
                            ) || job.post_state != PostState::Idle
                                || job.status == JobStatus::AwaitingQueueScripts)
                        {
                            Err(SchedulerError::Conflict(
                                "semantic cancellation is only allowed while queued or downloading"
                                    .into(),
                            ))
                        } else {
                            if delete_history && !row.intents.delete_history_after_cancel {
                                row.intents.delete_history_after_cancel = true;
                                changed = true;
                            }
                            if row.intents.cancelled.is_none() {
                                row.intents.cancelled = Some(origin);
                                row.intents.seq = row.intents.seq.wrapping_add(1);
                                changed = true;
                            }
                            Ok(())
                        }
                    }
                };
                result.insert(id, outcome);
            }
            if changed {
                Change::changed(result)
            } else {
                Change::unchanged(result)
            }
        })
    }

    pub fn update_job(&self, id: JobId, update: JobUpdate) -> Result<(), SchedulerError> {
        self.update_jobs([(id, update)])
            .remove(&id)
            .unwrap_or(Err(SchedulerError::JobNotFound(id)))
    }

    // Apply every update named, as one intent: one revision and one wake for
    // the whole batch, each job answered on its own.
    pub fn update_jobs(
        &self,
        updates: impl IntoIterator<Item = (JobId, JobUpdate)>,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
        self.write_intent(|inner| {
            let mut outcomes = BTreeMap::new();
            let mut changed = false;
            for (id, update) in updates {
                let outcome = update_row(inner, id, update);
                changed |= matches!(outcome, Ok(true));
                outcomes.insert(id, outcome.map(|_| ()));
            }
            if changed {
                Change::changed(outcomes)
            } else {
                Change::unchanged(outcomes)
            }
        })
    }

    pub fn reorder_jobs(&self, moves: Vec<(JobId, QueueMoveTarget)>) -> Result<(), SchedulerError> {
        self.write_intent(|inner| {
            let mut order: Vec<_> = inner
                .jobs
                .iter()
                .filter(|row| row.intents.cancelled.is_none())
                .map(|row| row.id)
                .collect();
            for (id, target) in moves {
                if let Err(error) = splice_job_order(&mut order, id, target) {
                    return Change::unchanged(Err(error));
                }
            }
            // The rows are laid out once in the new order, each found through
            // the position index; a cancelled row keeps its place after them.
            let mut rows: Vec<_> = std::mem::take(&mut inner.jobs)
                .into_iter()
                .map(Some)
                .collect();
            let mut jobs = Vec::with_capacity(rows.len());
            for id in order {
                if let Some(row) = inner.job_positions.get(&id).and_then(|at| rows[*at].take()) {
                    jobs.push(row);
                }
            }
            jobs.extend(rows.into_iter().flatten());
            inner.jobs = jobs;
            inner.reindex_jobs();
            Change::changed(Ok(()))
        })
    }
}

fn set_row_paused(
    inner: &mut super::QueueInner,
    job_id: JobId,
    paused: bool,
) -> Result<(), SchedulerError> {
    let Some(position) = inner.job_position(job_id) else {
        return Err(SchedulerError::JobNotFound(job_id));
    };
    let globals = &inner.globals;
    let row = &mut inner.jobs[position];
    if row.intents.cancelled.is_some() {
        return Err(SchedulerError::JobNotFound(job_id));
    }
    if row.report.retiring {
        return Err(SchedulerError::Conflict("job is being archived".into()));
    }
    if paused && row.report.moving {
        return Err(SchedulerError::Conflict(
            "pause is not supported while the final move is running".into(),
        ));
    }
    let job = row.info(globals);
    let extracting_with_download_work =
        job.post_state == PostState::Extracting && row.report.download_work_pending;
    if paused
        && job.status != JobStatus::Paused
        && ((!matches!(
            job.download_state,
            DownloadState::Queued | DownloadState::Downloading
        ) && !extracting_with_download_work)
            || job.status == JobStatus::AwaitingQueueScripts)
    {
        return Err(SchedulerError::Conflict(format!(
            "pause is only supported in queued or downloading states (current: {:?})",
            job.status
        )));
    }
    row.intents.paused = Some(paused);
    row.intents.seq = row.intents.seq.wrapping_add(1);
    row.intents.pause_seq = row.intents.seq;
    Ok(())
}

// Whether the update changed the row's intents.
fn update_row(
    inner: &mut super::QueueInner,
    id: JobId,
    update: JobUpdate,
) -> Result<bool, SchedulerError> {
    let Some(row) = inner
        .job_mut(id)
        .filter(|row| row.intents.cancelled.is_none())
    else {
        return Err(SchedulerError::JobNotFound(id));
    };
    if row.report.retiring {
        return Err(SchedulerError::Conflict("job is being archived".into()));
    }
    if update.is_empty() {
        return Ok(false);
    }
    let pending = row.intents.update.get_or_insert_with(JobUpdate::default);
    if !update.category.is_unchanged() {
        pending.category = update.category;
    }
    if !update.password.is_unchanged() {
        pending.password = update.password;
    }
    if !update.metadata.is_unchanged() {
        pending.metadata = update.metadata;
        pending.metadata_edits.clear();
    }
    for (key, value) in update.metadata_edits {
        pending
            .metadata_edits
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(&key));
        pending.metadata_edits.push((key, value));
    }
    if update.name.is_some() {
        row.intents.rename_to = update.name.clone();
        pending.name = update.name;
    }
    row.intents.seq = row.intents.seq.wrapping_add(1);
    Ok(true)
}

pub(super) fn apply_update(job: &mut crate::JobInfo, update: &JobUpdate) {
    match &update.category {
        FieldUpdate::Unchanged => {}
        FieldUpdate::Clear => job.category = None,
        FieldUpdate::Set(value) => job.category = Some(value.clone()),
    }
    match &update.password {
        FieldUpdate::Unchanged => {}
        FieldUpdate::Clear => job.password = None,
        FieldUpdate::Set(value) => job.password = Some(value.clone()),
    }
    update.apply_to_metadata(&mut job.metadata);
    if let Some(name) = &update.name {
        job.name = name.clone();
        crate::ingest::set_renamed_title(&mut job.metadata, name);
    }
}
