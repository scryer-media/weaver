// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Change, JobRow, Queue};
use crate::jobs::AddJobOptions;
use crate::jobs::model::{DownloadState, PostState, RunState};
use crate::{JobId, JobInfo, JobSpec, JobStatus, SchedulerError};
use weaver_server_base::events::model::PipelineEvent;

#[derive(Clone)]
pub struct AdmissionResources {
    pub db: crate::Database,
    pub intermediate_dir: PathBuf,
    pub complete_dir: PathBuf,
    pub config: crate::settings::SharedConfig,
}

impl AdmissionResources {
    // Every directory a job's staging directory can lie under, as the
    // categories stand now.
    pub(crate) async fn staging_roots(&self) -> Vec<PathBuf> {
        destination_roots(&self.complete_dir, &self.config.read().await.categories)
    }
}

pub struct PreparedAdd {
    // The job's files, with their article lists let go once its NZB is
    // stored: an add waiting to be installed holds nothing per article.
    pub spec: JobSpec,
    // What the lists held, in the spec's file order.
    pub summaries: crate::jobs::model::FileSummaries,
    pub options: AddJobOptions,
    pub working_dir: PathBuf,
    pub nzb_hash: [u8; 32],
    pub nzb_password_candidates: Option<Vec<crate::jobs::ArchivePasswordCandidate>>,
    pub restart: Option<RestartAdmission>,
}

pub struct RestartAdmission {
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
            if inner.contains_job(id) || inner.admissions.contains(&id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            let mut info = initial_info(
                id,
                &request.spec,
                request.restart.as_ref().map(|read| &read.summaries),
                request.job_hash,
                &request.working_dir,
                request.status == JobStatus::Paused,
            );
            info.download_wait_reason = Some("Restoring saved job".into());
            if let Some(read) = &request.restart {
                info.created_at_epoch_ms = read.created_at_epoch_ms;
            }
            let mut row = JobRow::new(info);
            row.report.restoring = true;
            row.report.on_disk = request.restart.as_ref().is_some_and(|read| read.on_disk);
            row.intents.restore = Some(request);
            inner.push_job(row);
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
            if inner.contains_job(id) || !inner.admissions.insert(id) {
                return Change::unchanged(Err(SchedulerError::JobExists(id)));
            }
            inner.sweep_listed.remove(&id);
            inner.sweep_warned.remove(&id);
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
                inner.push_job(row);
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
            Some(&self.summaries),
            self.nzb_hash,
            &self.working_dir,
            self.options.initially_paused,
        )
    }
}

// A restore whose outline was read while it waited out of the queue window:
// its row states the files read, with the figures a restore read with its
// outline shows, and the ticket goes back in the row to wait its turn.
pub(crate) fn outline_read(row: &mut JobRow, request: crate::jobs::handle::RestoreJobRequest) {
    let spec = &request.spec;
    let figures = LeanFigures::of(spec, request.restart.as_ref().map(|read| &read.summaries));
    let info = &mut row.report.job;
    info.name = spec.name.clone();
    info.total_bytes = spec.total_bytes;
    info.total_files = spec.files.len() as u32;
    info.progress = figures.progress();
    info.remaining_bytes = figures.remaining_bytes;
    info.wanted_bytes = figures.wanted_bytes;
    info.optional_recovery_bytes = figures.optional_recovery_bytes;
    info.optional_recovery_downloaded_bytes = figures.optional_recovery_downloaded_bytes;
    info.remaining_par_files = figures.remaining_par_files;
    info.password = spec.password.clone();
    row.intents.restore = Some(request);
}

// The figures a job waiting its turn shows, from its spec and the summaries
// of the article lists it let go, so the queue answers for a job outside the
// window without installing it or reading its lists back. A file with no
// summary counts as it is listed in the spec. As the pipeline counts them:
// the recovery volumes apart from what a run starts out to bring, an
// article not in hand still to come at its share of its file's bytes, and
// every PAR2 or PAR3 file, index included, with an article still to come.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LeanFigures {
    pub remaining_bytes: u64,
    pub wanted_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub remaining_par_files: u32,
}

impl LeanFigures {
    pub fn of(spec: &JobSpec, summaries: Option<&crate::jobs::model::FileSummaries>) -> Self {
        use weaver_model::files::FileRole;
        let mut figures = Self::default();
        for (index, file) in spec.files.iter().enumerate() {
            let summary = summaries
                .and_then(|summaries| summaries.0.get(index).copied())
                .unwrap_or_else(|| crate::jobs::model::FileSummary::of(file));
            let left = if summary.articles == 0 {
                0
            } else {
                (u128::from(summary.bytes) * u128::from(summary.remaining.min(summary.articles))
                    / u128::from(summary.articles)) as u64
            };
            if file.role.is_recovery() {
                figures.optional_recovery_bytes += summary.bytes;
                figures.optional_recovery_downloaded_bytes += summary.bytes - left;
            } else {
                figures.remaining_bytes += left;
                figures.wanted_bytes += summary.bytes;
            }
            if matches!(file.role, FileRole::Par2 { .. } | FileRole::Par3 { .. })
                && summary.remaining > 0
            {
                figures.remaining_par_files += 1;
            }
        }
        figures
    }

    // As the pipeline reports it: done is what is wanted less what is still
    // to come.
    pub(crate) fn progress(&self) -> f64 {
        if self.wanted_bytes == 0 {
            0.0
        } else {
            (self.wanted_bytes - self.remaining_bytes) as f64 / self.wanted_bytes as f64
        }
    }
}

pub fn initial_info(
    id: JobId,
    spec: &JobSpec,
    summaries: Option<&crate::jobs::model::FileSummaries>,
    hash: [u8; 32],
    working_dir: &std::path::Path,
    initially_paused: bool,
) -> JobInfo {
    let figures = LeanFigures::of(spec, summaries);
    JobInfo {
        job_id: id,
        protocol: crate::jobs::Protocol::Nzb,
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
        progress: figures.progress(),
        total_bytes: spec.total_bytes,
        downloaded_bytes: 0,
        optional_recovery_bytes: figures.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: figures.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: spec.files.len() as u32,
        completed_files: 0,
        remaining_par_files: figures.remaining_par_files,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: figures.remaining_bytes,
        wanted_bytes: figures.wanted_bytes,
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

// The directory a job's finished files are delivered under: its category's
// folder, else the complete directory. A category whose folder cannot be
// resolved stages under the complete directory; its move then says why it
// cannot deliver.
pub fn destination_root(
    complete_dir: &Path,
    categories: &[weaver_server_settings::categories::CategoryConfig],
    category: Option<&str>,
) -> PathBuf {
    weaver_server_settings::categories::completion_parent(complete_dir, categories, category)
        .unwrap_or_else(|_| complete_dir.to_path_buf())
}

pub use crate::jobs::working_dir::{staging_dir_for, staging_dir_on};

// [`staging_dir_on`] with the devices read on the spot, as the choice was
// made before the disk owner answered it: what a test compares the owner's
// choice with.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub fn delivery_staging_dir(
    intermediate_dir: &Path,
    destination_root: &Path,
    job_id: JobId,
) -> Option<PathBuf> {
    staging_dir_on(
        weaver_server_disk::runtime::fs::same_device(intermediate_dir, destination_root),
        destination_root,
        job_id,
    )
}

// Make the job's staging directory and mark it the job's, as its working
// directory is. Nothing under the destination is reached through a symbolic
// link.
pub fn make_staging_dir(staging_dir: &Path, job_id: JobId) -> std::io::Result<()> {
    crate::jobs::working_dir::make_job_staging_dir(staging_dir, job_id)
}

// Every directory a job's staging directory can lie under: the complete
// directory and each category's folder.
pub fn destination_roots(
    complete_dir: &Path,
    categories: &[weaver_server_settings::categories::CategoryConfig],
) -> Vec<PathBuf> {
    let mut seen = std::collections::HashSet::new();
    std::iter::once(complete_dir.to_path_buf())
        .chain(categories.iter().map(|category| {
            destination_root(
                complete_dir,
                std::slice::from_ref(category),
                Some(&category.name),
            )
        }))
        .filter(|root| seen.insert(root.clone()))
        .collect()
}
