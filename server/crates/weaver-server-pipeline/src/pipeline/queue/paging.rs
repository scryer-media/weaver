// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Read-only views of the queue for pages, facades and single-job answers.
//!
//! A page walks every row once without copying a row's strings or lists, and
//! copies only the rows it returns; a single-job answer finds its row through
//! the position index.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use super::{Globals, Intents, JobRow, QueueInner, Report};
use crate::jobs::JobId;
use crate::jobs::handle::{JobInfo, SharedPipelineState};
use crate::jobs::model::{DownloadState, JobStatus, RunState};
use crate::jobs::phase_progress::JobPhaseProgress;

impl SharedPipelineState {
    /// Runs `read` over the queue under its lock. `read` borrows rows and
    /// copies only what it returns; it never serialises, waits or does I/O.
    pub fn read_queue<T>(&self, read: impl FnOnce(&QueueInner) -> T) -> T {
        self.queue.read(read)
    }

    /// The history rows the queue publishes, shared rather than copied.
    pub fn history_rows(&self) -> Arc<[JobInfo]> {
        self.queue.history()
    }

    /// The queue's revision. Read inside [`Self::read_queue`], it names the
    /// rows that closure saw.
    pub fn queue_revision(&self) -> u64 {
        self.queue.revision()
    }
}

impl QueueInner {
    /// The row for `id`, found through the position index.
    pub fn row(&self, id: JobId) -> Option<&JobRow> {
        self.job(id)
    }
}

/// One row as [`JobRow::info`] presents it, borrowed from the row.
///
/// The state fields `info` rewrites (status, run, download, wait reason,
/// retry time) come from `info` itself, run over a copy of the row that
/// carries no strings or lists, so `info` stays their one owner. A row with
/// an edit the pipeline has not applied yet is the exception: its name,
/// category and metadata change too, so it is copied whole through `info`.
pub struct JobView<'a> {
    job: Cow<'a, JobInfo>,
    // `info`'s answer for the state fields, when `job` is the stored report.
    derived: Option<JobInfo>,
    phases: &'a [JobPhaseProgress],
}

impl JobRow {
    pub fn view(&self, globals: &Globals) -> JobView<'_> {
        if self.intents.update.is_some() {
            return JobView {
                job: Cow::Owned(self.info(globals)),
                derived: None,
                phases: &[],
            };
        }
        let stored = &self.report.job;
        let bare = JobRow {
            id: self.id,
            intents: Intents {
                paused: self.intents.paused,
                pause_seq: self.intents.pause_seq,
                cancelled: self.intents.cancelled,
                delete_history_after_cancel: self.intents.delete_history_after_cancel,
                update: None,
                rename_to: None,
                restore: None,
                cancel_post_processing: self.intents.cancel_post_processing,
                seq: self.intents.seq,
            },
            report: Report {
                job: state_only(stored),
                nzb_admitted: self.report.nzb_admitted,
                seen_intent_seq: self.report.seen_intent_seq,
                retiring: self.report.retiring,
                finished_status: self.report.finished_status.clone(),
                restoring: self.report.restoring,
                on_disk: self.report.on_disk,
                post_processing_cancellable: self.report.post_processing_cancellable,
                moving: self.report.moving,
                download_active: self.report.download_active,
                download_work_pending: self.report.download_work_pending,
                observed_byte_at_ms: self.report.observed_byte_at_ms,
                propagation_until_epoch_ms: self.report.propagation_until_epoch_ms,
            },
            counters: self.counters.clone(),
            phases: HashMap::new(),
            sampled_phases: Vec::new(),
            download_rate: None,
            admission: None,
        };
        JobView {
            job: Cow::Borrowed(stored),
            derived: Some(bare.info(globals)),
            phases: if self.phases.is_empty() {
                &stored.phase_progress
            } else {
                &self.sampled_phases
            },
        }
    }
}

// The stored job's scalar state, with every string and list left empty, so
// copying it allocates nothing. The wait reason and retry time start empty
// too: whatever `info` puts there is its own, and otherwise the stored ones
// stand.
pub fn state_only(job: &JobInfo) -> JobInfo {
    JobInfo {
        job_id: job.job_id,
        protocol: job.protocol,
        job_hash: None,
        name: String::new(),
        status: job.status.clone(),
        download_state: job.download_state,
        finalizing_download: job.finalizing_download,
        fetching_repair_data: job.fetching_repair_data,
        post_state: job.post_state,
        run_state: job.run_state,
        progress: job.progress,
        total_bytes: job.total_bytes,
        downloaded_bytes: job.downloaded_bytes,
        optional_recovery_bytes: job.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: job.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: job.failed_bytes,
        health: job.health,
        terminal_discards: Vec::new(),
        total_files: job.total_files,
        completed_files: job.completed_files,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: job.remaining_bytes,
        wanted_bytes: job.wanted_bytes,
        remaining_par_files: job.remaining_par_files,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        server_attribution: Vec::new(),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        disk_need_bytes: None,
        disk_free_bytes: None,
        created_at_epoch_ms: job.created_at_epoch_ms,
    }
}

impl JobView<'_> {
    /// The job's own fields: name, category, metadata, sizes and the rest.
    /// Read the state fields through the accessors below, not from here.
    pub fn job(&self) -> &JobInfo {
        &self.job
    }

    fn state(&self) -> &JobInfo {
        self.derived.as_ref().unwrap_or(&self.job)
    }

    pub fn status(&self) -> &JobStatus {
        &self.state().status
    }

    pub fn run_state(&self) -> RunState {
        self.state().run_state
    }

    pub fn download_state(&self) -> DownloadState {
        self.state().download_state
    }

    pub fn download_wait_reason(&self) -> Option<&str> {
        self.state()
            .download_wait_reason
            .as_deref()
            .or(self.job.download_wait_reason.as_deref())
    }

    pub fn download_retry_at_epoch_ms(&self) -> Option<f64> {
        self.state()
            .download_retry_at_epoch_ms
            .or(self.job.download_retry_at_epoch_ms)
    }

    pub fn phase_progress(&self) -> &[JobPhaseProgress] {
        match &self.job {
            Cow::Owned(job) => &job.phase_progress,
            Cow::Borrowed(_) => self.phases,
        }
    }

    /// The row as [`JobRow::info`] presents it, less the per-file remaining
    /// bytes, server attribution and terminal discards: what a listing copies.
    pub fn list_info(&self) -> JobInfo {
        let job = self.job();
        JobInfo {
            job_id: job.job_id,
            protocol: job.protocol,
            job_hash: job.job_hash,
            name: job.name.clone(),
            status: self.status().clone(),
            download_state: self.download_state(),
            finalizing_download: job.finalizing_download,
            fetching_repair_data: job.fetching_repair_data,
            post_state: job.post_state,
            run_state: self.run_state(),
            progress: job.progress,
            total_bytes: job.total_bytes,
            downloaded_bytes: job.downloaded_bytes,
            optional_recovery_bytes: job.optional_recovery_bytes,
            optional_recovery_downloaded_bytes: job.optional_recovery_downloaded_bytes,
            phase_progress: self.phase_progress().to_vec(),
            failed_bytes: job.failed_bytes,
            health: job.health,
            terminal_discards: Vec::new(),
            total_files: job.total_files,
            completed_files: job.completed_files,
            file_remaining_bytes: Vec::new(),
            file_parked_bytes: Vec::new(),
            remaining_bytes: job.remaining_bytes,
            wanted_bytes: job.wanted_bytes,
            remaining_par_files: job.remaining_par_files,
            password: job.password.clone(),
            category: job.category.clone(),
            metadata: job.metadata.clone(),
            output_dir: job.output_dir.clone(),
            server_attribution: Vec::new(),
            error: job.error.clone(),
            download_wait_reason: self.download_wait_reason().map(str::to_owned),
            download_retry_at_epoch_ms: self.download_retry_at_epoch_ms(),
            // Kept on the row as the pipeline restates it: no read of its own.
            disk_need_bytes: job.disk_need_bytes,
            disk_free_bytes: job.disk_free_bytes,
            created_at_epoch_ms: job.created_at_epoch_ms,
        }
    }
}
