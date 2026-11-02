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
                restoring: self.report.restoring,
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
fn state_only(job: &JobInfo) -> JobInfo {
    JobInfo {
        job_id: job.job_id,
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
        remaining_par_files: job.remaining_par_files,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        server_attribution: Vec::new(),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
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
            remaining_par_files: job.remaining_par_files,
            password: job.password.clone(),
            category: job.category.clone(),
            metadata: job.metadata.clone(),
            output_dir: job.output_dir.clone(),
            server_attribution: Vec::new(),
            error: job.error.clone(),
            download_wait_reason: self.download_wait_reason().map(str::to_owned),
            download_retry_at_epoch_ms: self.download_retry_at_epoch_ms(),
            created_at_epoch_ms: job.created_at_epoch_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::handle::CancellationOrigin;
    use crate::jobs::model::{FieldUpdate, PostState};
    use crate::jobs::phase_progress::JobPhase;
    use crate::jobs::{JobUpdate, epoch_ms_now};

    fn job(id: u64, status: JobStatus, download: DownloadState, post: PostState) -> JobInfo {
        let mut job = state_only(&JobInfo {
            job_id: JobId(id),
            job_hash: None,
            name: String::new(),
            status,
            download_state: download,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: post,
            run_state: RunState::Active,
            progress: 0.25,
            total_bytes: 1000,
            downloaded_bytes: 250,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 3,
            completed_files: 1,
            file_remaining_bytes: Vec::new(),
            remaining_par_files: 0,
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            server_attribution: Vec::new(),
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            created_at_epoch_ms: 1.0,
        });
        job.name = format!("Release.{id}.1080p");
        job.category = Some("tv".into());
        job.metadata = vec![("weaver.priority".into(), "HIGH".into())];
        job.file_remaining_bytes = vec![10, 20, 30];
        job
    }

    // Every row shape `info` treats differently, under every global hold.
    fn rows() -> Vec<JobRow> {
        let mut rows = Vec::new();
        let shapes = [
            (JobStatus::Queued, DownloadState::Queued, PostState::Idle),
            (
                JobStatus::Downloading,
                DownloadState::Downloading,
                PostState::Idle,
            ),
            (
                JobStatus::Downloading,
                DownloadState::Queued,
                PostState::Idle,
            ),
            (JobStatus::Paused, DownloadState::Queued, PostState::Idle),
            (
                JobStatus::Extracting,
                DownloadState::Complete,
                PostState::Extracting,
            ),
            (
                JobStatus::AwaitingQueueScripts,
                DownloadState::Queued,
                PostState::Idle,
            ),
        ];
        let mut id = 0;
        for (status, download, post) in shapes {
            for variant in 0..8u8 {
                id += 1;
                let mut row = JobRow::new(job(id, status.clone(), download, post));
                row.report.download_active = variant & 1 != 0;
                row.report.download_work_pending = variant & 2 != 0;
                if variant & 4 != 0 {
                    row.report.propagation_until_epoch_ms = Some(epoch_ms_now() + 3_600_000.0);
                    row.report.job.download_wait_reason = Some("nzb_slot".into());
                }
                match variant % 3 {
                    0 => {}
                    1 => {
                        row.intents.paused = Some(true);
                        row.intents.seq = 1;
                        row.intents.pause_seq = 1;
                    }
                    _ => {
                        row.intents.paused = Some(false);
                        row.intents.seq = 2;
                        row.intents.pause_seq = 2;
                        row.report.seen_intent_seq = 1;
                    }
                }
                if variant == 5 {
                    row.counters
                        .last_byte_at_ms
                        .store(10, std::sync::atomic::Ordering::Relaxed);
                }
                if variant == 6 {
                    row.sampled_phases = vec![JobPhaseProgress {
                        phase: JobPhase::Downloading,
                        completed_bytes: 1,
                        total_bytes: 2,
                        progress_percent: 50.0,
                        rate_bps: Some(7),
                        estimated_remaining_ms: None,
                        started_at_epoch_ms: 0.0,
                        updated_at_epoch_ms: 0.0,
                    }];
                }
                if variant == 7 {
                    row.intents.cancelled = Some(CancellationOrigin::User);
                    row.intents.update = Some(JobUpdate {
                        category: FieldUpdate::Set("movies".into()),
                        name: Some("Renamed".into()),
                        ..JobUpdate::default()
                    });
                }
                rows.push(row);
            }
        }
        rows
    }

    #[test]
    fn a_view_reads_every_state_field_as_info_does() {
        let holds = [
            Globals::default(),
            Globals {
                paused: true,
                ..Globals::default()
            },
            Globals {
                speed_test_paused: true,
                ..Globals::default()
            },
        ];
        let queue = super::super::Queue::new(Vec::new());
        let rows = rows();
        let sampled: Vec<JobId> = rows
            .iter()
            .filter(|row| !row.sampled_phases.is_empty())
            .map(|row| row.id)
            .collect();
        queue.write(|inner| {
            for row in rows {
                inner.push_job(row);
            }
            super::super::Change::changed(())
        });
        // A phase the pipeline samples replaces the stored phase progress.
        for id in sampled {
            queue.begin_phase(id, JobPhase::Downloading, Arc::default(), 0.0);
        }
        for globals in &holds {
            queue.read(|inner| {
                for row in &inner.jobs {
                    let info = row.info(globals);
                    let view = row.view(globals);
                    let id = row.id;
                    assert_eq!(view.status(), &info.status, "{id:?}");
                    assert_eq!(view.run_state(), info.run_state, "{id:?}");
                    assert_eq!(view.download_state(), info.download_state, "{id:?}");
                    assert_eq!(
                        view.download_wait_reason(),
                        info.download_wait_reason.as_deref(),
                        "{id:?}"
                    );
                    assert_eq!(
                        view.download_retry_at_epoch_ms(),
                        info.download_retry_at_epoch_ms,
                        "{id:?}"
                    );
                    assert_eq!(view.phase_progress(), info.phase_progress.as_slice());
                    let (seen, owned) = (view.job(), &info);
                    assert_eq!(seen.name, owned.name);
                    assert_eq!(seen.category, owned.category);
                    assert_eq!(seen.metadata, owned.metadata);
                    assert_eq!(seen.post_state, owned.post_state);
                    assert_eq!(seen.total_bytes, owned.total_bytes);
                }
            });
        }
    }

    #[test]
    fn a_row_is_found_through_the_position_index() {
        let queue = super::super::Queue::new(
            (1..=5)
                .map(|id| {
                    job(
                        id,
                        JobStatus::Queued,
                        DownloadState::Queued,
                        PostState::Idle,
                    )
                })
                .collect(),
        );
        queue.read(|inner| {
            assert_eq!(inner.row(JobId(4)).map(|row| row.id), Some(JobId(4)));
            assert!(inner.row(JobId(9)).is_none());
        });
    }
}
