// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::{Change, Queue};
use crate::jobs::handle::{
    CancelJobsOutcome, CancellationOrigin, QueueMoveTarget, splice_job_order,
};
use crate::jobs::model::{DownloadState, FieldUpdate, JobStatus, PostState};
use crate::jobs::{JobId, JobUpdate, SchedulerError};

impl Queue {
    pub fn set_job_paused(&self, job_id: JobId, paused: bool) -> Result<(), SchedulerError> {
        self.write_intent(|inner| {
            let Some(row) = inner
                .jobs
                .iter_mut()
                .find(|row| row.id == job_id && row.intents.cancelled.is_none())
            else {
                return Change::unchanged(Err(SchedulerError::JobNotFound(job_id)));
            };
            if row.report.retiring {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "job is being archived".into(),
                )));
            }
            if paused && row.report.moving {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "pause is not supported while the final move is running".into(),
                )));
            }
            let job = row.info(&inner.globals);
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
                return Change::unchanged(Err(SchedulerError::Conflict(format!(
                    "pause is only supported in queued or downloading states (current: {:?})",
                    job.status
                ))));
            }
            row.intents.paused = Some(paused);
            row.intents.seq = row.intents.seq.wrapping_add(1);
            row.intents.pause_seq = row.intents.seq;
            Change::changed(Ok(()))
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
                let outcome = match inner.jobs.iter_mut().find(|row| row.id == id) {
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
        self.write_intent(|inner| {
            let Some(row) = inner
                .jobs
                .iter_mut()
                .find(|row| row.id == id && row.intents.cancelled.is_none())
            else {
                return Change::unchanged(Err(SchedulerError::JobNotFound(id)));
            };
            if row.report.retiring {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "job is being archived".into(),
                )));
            }
            if update.is_empty() {
                return Change::unchanged(Ok(()));
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
            Change::changed(Ok(()))
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
            inner.jobs.sort_by_key(|row| {
                order
                    .iter()
                    .position(|id| *id == row.id)
                    .unwrap_or(usize::MAX)
            });
            Change::changed(Ok(()))
        })
    }
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
