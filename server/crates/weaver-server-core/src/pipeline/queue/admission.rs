// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::sync::Arc;

use super::{Change, JobRow, Queue};
use crate::events::model::PipelineEvent;
use crate::jobs::AddJobOptions;
use crate::jobs::model::{DownloadState, PostState, RunState};
use crate::{JobId, JobInfo, JobSpec, JobStatus, SchedulerError};

#[derive(Clone)]
pub(crate) struct AdmissionResources {
    pub db: crate::Database,
    pub intermediate_dir: PathBuf,
    pub complete_dir: PathBuf,
    pub memory: Arc<crate::pipeline::ProcessMemoryBudget>,
    // The engine an admitted job is stamped with: the process switch, so
    // graph unless the server opted into classic.
    pub engine: crate::pipeline::graph::Engine,
}

pub(crate) struct PreparedAdd {
    pub spec: JobSpec,
    pub options: AddJobOptions,
    pub working_dir: PathBuf,
    pub nzb_hash: [u8; 32],
    pub nzb_password_candidates: Option<Vec<crate::jobs::ArchivePasswordCandidate>>,
    pub scheduling_memory: crate::pipeline::ProcessMemoryPermit,
    pub repeated_articles: Option<Arc<crate::pipeline::download::repeated::RepeatedArticles>>,
    pub restart: Option<RestartAdmission>,
    pub engine: crate::pipeline::graph::Engine,
}

pub(crate) struct RestartAdmission {
    pub reprocess: bool,
    pub working_dir: PathBuf,
    pub downloaded_bytes: u64,
}

struct Reservation {
    queue: Arc<Queue>,
    id: JobId,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.queue.write(|inner| {
            inner.admissions.remove(&self.id);
            Change::changed(())
        });
    }
}

impl Queue {
    pub(crate) fn restore(
        &self,
        request: crate::jobs::handle::RestoreJobRequest,
    ) -> Result<(), SchedulerError> {
        self.write_intent(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            let id = request.job_id;
            if inner.jobs.iter().any(|row| row.id == id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            let mut info = initial_info(
                id,
                &request.spec,
                request.job_hash,
                &request.working_dir,
                request.status == JobStatus::Paused,
            );
            info.download_wait_reason = Some("Restoring saved job".into());
            let mut row = JobRow::new(info);
            row.report.restoring = true;
            row.intents.restore = Some(request);
            inner.jobs.push(row);
            Change::changed(Ok(()))
        })
    }

    pub(crate) async fn add(
        self: Arc<Self>,
        id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: AddJobOptions,
        events: tokio::sync::broadcast::Sender<PipelineEvent>,
    ) -> Result<(), SchedulerError> {
        let mut resources = self
            .resources
            .get()
            .cloned()
            .ok_or(SchedulerError::ChannelClosed)?;
        if let Some(paths) = self.read(|inner| inner.globals.runtime_paths.clone()) {
            resources.intermediate_dir = paths.intermediate;
        }
        self.write(|inner| {
            if inner.globals.shutting_down || self.closed.load(std::sync::atomic::Ordering::Acquire)
            {
                return Change::unchanged(Err(SchedulerError::ChannelClosed));
            }
            if inner.history_operations.contains(&id) {
                return Change::unchanged(Err(SchedulerError::Conflict(
                    "a history operation is still running".into(),
                )));
            }
            if inner.jobs.iter().any(|row| row.id == id) || !inner.admissions.insert(id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            Change::unchanged(Ok(()))
        })?;
        let reservation = Reservation {
            queue: self.clone(),
            id,
        };
        // Completion belongs to the admission, even if the HTTP request is
        // cancelled after the durable insert has begun.
        tokio::spawn(async move {
            let _reservation = reservation;
            self.wait_for_cleanup(&std::collections::BTreeSet::from([id]))
                .await;
            let prepared = crate::pipeline::Pipeline::prepare_add(
                &resources, id, spec, nzb_path, nzb_zstd, options,
            )
            .await?;
            let job = prepared.info(id);
            let event = PipelineEvent::JobCreated {
                job_id: id,
                name: job.name.clone(),
                total_files: job.total_files,
                total_bytes: job.total_bytes,
            };
            self.write_intent(|inner| {
                let mut row = JobRow::new(job);
                row.admission = Some(prepared);
                inner.jobs.push(row);
                let _ = events.send(event);
                Change::changed(())
            });
            Ok(())
        })
        .await
        .map_err(|error| SchedulerError::Internal(format!("admission worker failed: {error}")))?
    }

    pub(crate) async fn finish_admissions(&self) {
        let mut revision = self.subscribe();
        while self.read(|inner| !inner.admissions.is_empty()) {
            if revision.changed().await.is_err() {
                break;
            }
        }
    }
}

impl PreparedAdd {
    fn info(&self, id: JobId) -> JobInfo {
        initial_info(
            id,
            &self.spec,
            self.nzb_hash,
            &self.working_dir,
            self.options.initially_paused,
        )
    }
}

pub(super) fn initial_info(
    id: JobId,
    spec: &JobSpec,
    hash: [u8; 32],
    working_dir: &std::path::Path,
    initially_paused: bool,
) -> JobInfo {
    JobInfo {
        job_id: id,
        job_hash: Some(hash),
        name: spec.name.clone(),
        status: if initially_paused {
            JobStatus::Paused
        } else {
            JobStatus::Queued
        },
        download_state: DownloadState::Queued,
        post_state: PostState::Idle,
        run_state: if initially_paused {
            RunState::Paused
        } else {
            RunState::Active
        },
        finalizing_download: false,
        fetching_repair_data: false,
        progress: 0.0,
        total_bytes: spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: spec.files.len() as u32,
        completed_files: 0,
        remaining_par_files: 0,
        password: spec.password.clone(),
        category: spec.category.clone(),
        metadata: spec.metadata.clone(),
        output_dir: Some(working_dir.display().to_string()),
        server_attribution: Vec::new(),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
    }
}
