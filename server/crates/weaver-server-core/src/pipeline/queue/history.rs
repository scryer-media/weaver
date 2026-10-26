// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{Change, Queue};
use crate::{HistoryDeleteBatchOutcome, JobId, SchedulerError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(crate) async fn clear_restart_artifacts(
    resources: &super::AdmissionResources,
    id: JobId,
    working_dir: &std::path::Path,
) -> Result<(), SchedulerError> {
    // The reservation and memory preflight precede cleanup. History paths
    // alone are never permission to remove an arbitrary directory.
    crate::post_processing::events::wait_for_job_events_stopped(&resources.db, id.0).await?;
    if tokio::fs::try_exists(working_dir).await? {
        if working_dir.starts_with(&resources.intermediate_dir) {
            use crate::jobs::working_dir::{
                HistoryWorkingDir, prepare_history_working_dir, remove_job_working_dir,
            };
            if prepare_history_working_dir(&resources.intermediate_dir, working_dir, id).await?
                != HistoryWorkingDir::Owned
            {
                return Err(SchedulerError::Conflict(
                    "restart directory belongs to another job".into(),
                ));
            }
            let root = resources.intermediate_dir.clone();
            let path = working_dir.to_path_buf();
            let removed =
                tokio::task::spawn_blocking(move || remove_job_working_dir(&root, &path, id))
                    .await
                    .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            if removed != HistoryWorkingDir::Owned {
                return Err(SchedulerError::Conflict(
                    "restart directory ownership changed".into(),
                ));
            }
        } else {
            let path = working_dir.to_path_buf();
            let owned = tokio::task::spawn_blocking(move || {
                crate::jobs::working_dir::is_weaver_owned_output_dir(&path)
            })
            .await
            .map_err(|error| SchedulerError::Internal(error.to_string()))?;
            if !owned {
                return Err(SchedulerError::Conflict(
                    "restart output directory has no valid ownership marker".into(),
                ));
            }
            tokio::fs::remove_dir_all(working_dir).await?;
        }
    }
    let staging = resources
        .complete_dir
        .join(".weaver-staging")
        .join(id.0.to_string());
    match tokio::fs::symlink_metadata(&staging).await {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(SchedulerError::Conflict(
                "restart staging directory is a symbolic link".into(),
            ));
        }
        Ok(_) => {
            tokio::fs::remove_dir_all(staging).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

struct HistoryOperation {
    queue: Arc<Queue>,
    ids: BTreeSet<JobId>,
}

impl Drop for HistoryOperation {
    fn drop(&mut self) {
        self.queue.write(|inner| {
            for id in &self.ids {
                inner.history_operations.remove(id);
                inner.admissions.remove(id);
            }
            Change::changed(())
        });
    }
}

impl Queue {
    pub(super) async fn wait_for_cleanup(&self, ids: &BTreeSet<JobId>) {
        let mut revision = self.subscribe();
        while self.read(|inner| ids.iter().any(|id| inner.pending_cleanup.contains(id))) {
            if revision.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn restart(
        self: Arc<Self>,
        id: JobId,
        password: Option<String>,
        reprocess: bool,
        events: tokio::sync::broadcast::Sender<crate::events::model::PipelineEvent>,
    ) -> Result<(), SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
            resources.complete_dir = paths.complete;
        }
        self.write(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            if inner.jobs.iter().any(|row| row.id == id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "job is still in the queue".into(),
                )));
            }
            if !inner.history_operations.insert(id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            inner.admissions.insert(id);
            Change::unchanged(Ok(()))
        })?;
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: BTreeSet::from([id]),
        };
        tokio::spawn(async move {
            let _guard = guard;
            self.wait_for_cleanup(&BTreeSet::from([id])).await;
            let db = resources.db.clone();
            let (spec, path, bytes, previous_dir, downloaded_bytes) =
                tokio::task::spawn_blocking(move || {
                    let row = db
                        .get_job_history_profiled(id.0, "queue.restart.history")?
                        .ok_or(SchedulerError::JobNotFound(id))?;
                    let status = crate::job_status_from_persisted_str(
                        &row.status,
                        row.error_message.as_deref(),
                    );
                    if !matches!(
                        status,
                        crate::JobStatus::Complete
                            | crate::JobStatus::Partial
                            | crate::JobStatus::Failed { .. }
                    ) {
                        return Err(SchedulerError::Conflict(
                            "job is not complete or failed".into(),
                        ));
                    }
                    let (path, bytes) = db
                        .load_history_job_persisted_nzb(id.0)?
                        .ok_or(SchedulerError::JobNotFound(id))?;
                    let bytes = bytes.ok_or_else(|| {
                        SchedulerError::InvalidInput("persisted NZB is missing".into())
                    })?;
                    let nzb =
                        crate::ingest::parse_persisted_nzb_bytes(&bytes).map_err(|error| {
                            SchedulerError::InvalidInput(format!("invalid persisted NZB: {error}"))
                        })?;
                    let mut metadata: Vec<(String, String)> = row
                        .metadata
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|error| SchedulerError::Internal(error.to_string()))?
                        .unwrap_or_default();
                    metadata.retain(|(key, _)| {
                        !crate::history::attributes::is_password_attribute_key(key)
                            && (reprocess || key != crate::ingest::FULL_FILE_NAMES_METADATA_KEY)
                    });
                    let mut spec = crate::ingest::nzb_to_spec(&nzb, &path, row.category, metadata);
                    if let Some(password) = password {
                        spec.password = Some(password);
                    }
                    Ok::<_, SchedulerError>((
                        spec,
                        path,
                        bytes,
                        row.output_dir,
                        row.downloaded_bytes,
                    ))
                })
                .await
                .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            let working_dir = previous_dir
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    crate::jobs::working_dir::compute_working_dir(
                        &resources.intermediate_dir,
                        id,
                        &spec.name,
                    )
                });
            let prepared = crate::pipeline::Pipeline::prepare_admission(
                &resources,
                id,
                spec,
                path,
                bytes,
                crate::jobs::AddJobOptions::default(),
                Some(super::RestartAdmission {
                    reprocess,
                    working_dir,
                    downloaded_bytes,
                }),
            )
            .await?;
            let info = super::admission::initial_info(
                id,
                &prepared.spec,
                prepared.nzb_hash,
                &prepared.working_dir,
                false,
            );
            self.write_intent(|inner| {
                let history = self
                    .history()
                    .iter()
                    .filter(|job| job.job_id != id)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into();
                self.replace_history(inner, history);
                let mut row = super::JobRow::new(info);
                row.admission = Some(prepared);
                inner.push_job(row);
                let _ = events.send(crate::events::model::PipelineEvent::JobResumed { job_id: id });
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(error.to_string()))?
    }

    pub(crate) async fn update_history(
        self: Arc<Self>,
        id: JobId,
        mut update: crate::jobs::JobUpdate,
    ) -> Result<(), SchedulerError> {
        let db = self
            .resources
            .get()
            .ok_or(SchedulerError::ChannelClosed)?
            .db
            .clone();
        self.write(|inner| {
            if inner.jobs.iter().any(|row| row.id == id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "the job is still in the queue".into(),
                )));
            }
            if !inner.history_operations.insert(id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            Change::unchanged(Ok(()))
        })?;
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: BTreeSet::from([id]),
        };
        update.password = crate::jobs::FieldUpdate::Unchanged;
        tokio::spawn(async move {
            let _guard = guard;
            let persisted = update.clone();
            let found =
                tokio::task::spawn_blocking(move || db.update_job_history_fields(id.0, &persisted))
                    .await
                    .map_err(|error| SchedulerError::Internal(error.to_string()))??;
            if !found {
                return Err(SchedulerError::JobNotFound(id));
            }
            self.write(|inner| {
                let mut history = self.history().to_vec();
                if let Some(job) = history.iter_mut().find(|job| job.job_id == id) {
                    super::intents::apply_update(job, &update);
                }
                self.replace_history(inner, history.into());
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(error.to_string()))?
    }

    pub(crate) async fn delete_history(
        self: Arc<Self>,
        ids: BTreeSet<JobId>,
        delete_files: bool,
    ) -> Result<HistoryDeleteBatchOutcome, SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
        }
        let (accepted, refused) = self.write(|inner| {
            let mut accepted = BTreeSet::new();
            let mut refused = BTreeMap::new();
            for id in ids {
                if inner.jobs.iter().any(|row| row.id == id) || inner.admissions.contains(&id) {
                    refused.insert(
                        id,
                        SchedulerError::Conflict(
                            "cannot delete active job — cancel it first".into(),
                        ),
                    );
                } else if !inner.history_operations.insert(id) {
                    refused.insert(
                        id,
                        SchedulerError::Conflict("history deletion is still running".into()),
                    );
                } else {
                    accepted.insert(id);
                }
            }
            Change::unchanged((accepted, refused))
        });
        if accepted.is_empty() {
            return Ok(HistoryDeleteBatchOutcome {
                deleted: BTreeMap::new(),
                refused,
            });
        }
        let guard = HistoryOperation {
            queue: self.clone(),
            ids: accepted.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            self.wait_for_cleanup(&accepted).await;
            let (removed, result) = crate::history::delete_files::delete_history_rows(
                &resources.db,
                &resources.intermediate_dir,
                accepted,
                delete_files,
            )
            .await;
            self.write(|inner| {
                let history = self
                    .history()
                    .iter()
                    .filter(|job| !removed.contains(&job.job_id))
                    .cloned()
                    .collect::<Vec<_>>()
                    .into();
                self.replace_history(inner, history);
                Change::changed(())
            });
            let mut outcome = result?;
            outcome.refused.extend(refused);
            Ok(outcome)
        })
        .await
        .map_err(|error| {
            SchedulerError::Internal(format!("history delete worker failed: {error}"))
        })?
    }
}
