// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::queue::Change;

impl Pipeline {
    pub(crate) fn report_job(&self, job_id: JobId) {
        self.report_job_event(job_id, None);
    }

    fn report_job_event(&self, job_id: JobId, event: Option<PipelineEvent>) {
        let report = self.job_report(job_id);
        let moving = self.inflight_moves.contains(&job_id);
        let download_active = self.job_has_current_download_activity(job_id);
        let download_work_pending = self.job_has_pending_download_pipeline_work(job_id);
        let propagation_until = self
            .propagation_ready_at
            .get(&job_id)
            .map(|(_, epoch)| *epoch as f64);
        let cancellable = self
            .terminal_post_processing_cancellations
            .contains_key(&job_id)
            || self
                .terminal_post_processing_executor
                .has_cancellable_job(job_id.0);
        self.shared_state.queue.write(|inner| {
            if let Some(job) = report {
                let row = if let Some(index) = inner.jobs.iter().position(|row| row.id == job_id) {
                    let row = &mut inner.jobs[index];
                    row.report.job = job;
                    row
                } else {
                    return Change::unchanged(());
                };
                row.report.moving = moving;
                row.report.nzb_admitted = self.nzb_admitted.contains(&job_id);
                if !self.nzb_admitted.contains(&job_id)
                    && row.intents.paused == Some(false)
                    && row.intents.cancelled.is_none()
                {
                    row.report.job.download_wait_reason =
                        Some(crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON.into());
                    row.report.job.download_state = crate::DownloadState::Queued;
                }
                row.report.download_active = download_active;
                row.report.download_work_pending = download_work_pending;
                row.report.observed_byte_at_ms = row
                    .counters
                    .last_byte_at_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                row.report.propagation_until_epoch_ms = propagation_until;
                row.report.post_processing_cancellable = cancellable;
            }
            if let Some(event) = event {
                let _ = self.event_tx.send(event);
            }
            Change::changed(())
        });
    }

    pub(crate) fn emit_job_event(&self, event: PipelineEvent) {
        if let Some(id) = crate::events::publish::pipeline_job_id(&event) {
            self.report_job_event(JobId(id), Some(event));
        } else {
            self.shared_state.queue.write(|_| {
                let _ = self.event_tx.send(event);
                Change::changed(())
            });
        }
    }

    // The snapshot walks the actor's maps, so it is built only when the
    // diagnostics endpoint has asked for one since the last answer.
    pub(crate) fn answer_diagnostics_request(&self) {
        let requested = self.shared_state.queue.read(|inner| {
            (inner.diagnostics_requested
                > inner
                    .diagnostics
                    .as_ref()
                    .map_or(0, |(answered, _)| *answered))
            .then_some(inner.diagnostics_requested)
        });
        let Some(requested) = requested else {
            return;
        };
        let diagnostics = self.diagnostics_snapshot();
        self.shared_state.queue.write(|inner| {
            inner.diagnostics = Some((requested, diagnostics));
            Change::changed(())
        });
    }

    #[cfg(test)]
    pub(crate) async fn finished_jobs(&self) -> Vec<JobInfo> {
        let mut changes = self.shared_state.queue.subscribe();
        self.db.flush_write_queue().await.expect("history writer");
        loop {
            let history = self.shared_state.queue.read(|inner| {
                (!inner.jobs.iter().any(|row| row.report.retiring))
                    .then(|| self.shared_state.queue.history().to_vec())
            });
            if let Some(history) = history {
                return history;
            }
            changes.changed().await.expect("history publication");
        }
    }

    #[cfg(test)]
    pub(crate) fn report_all_for_test(&self) {
        let jobs = self
            .job_order
            .iter()
            .filter_map(|id| self.job_report(*id))
            .collect();
        self.shared_state.queue.report_jobs(jobs);
        for (&(id, phase), runtime) in &self.phase_progress {
            self.shared_state.queue.begin_phase(
                id,
                phase,
                runtime.counters.clone(),
                runtime.started_at_epoch_ms,
            );
        }
        for id in &self.job_order {
            self.report_job(*id);
        }
    }
}

impl Pipeline {
    // Build a report for the one job whose stage changed.
    pub(crate) fn job_report(&self, job_id: JobId) -> Option<JobInfo> {
        let state = self.jobs.get(&job_id)?;
        if is_terminal_status(&state.status) {
            return None;
        }
        let total = state.spec.total_bytes;
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.optional_recovery_bytes();
        let health = health_milli(total, state.failed_bytes);
        let status = if self.awaiting_queue_script_barrier(state.job_id)
            && state.status == JobStatus::Downloading
        {
            // The download is complete; its queue scripts are being admitted.
            JobStatus::AwaitingQueueScripts
        } else {
            state.status.clone()
        };
        let (download_state, post_state, run_state) =
            crate::jobs::model::runtime_lanes_from_status_snapshot(&status);
        let is_par_volume = |role: &weaver_model::files::FileRole| {
            matches!(
                role,
                weaver_model::files::FileRole::Par2 {
                    is_index: false,
                    ..
                }
            )
        };
        let remaining_par_files = match state.articles.assembly() {
            Some(assembly) => assembly
                .files()
                .filter(|file| is_par_volume(file.role()) && !file.is_complete())
                .count() as u32,
            None => state
                .spec
                .files
                .iter()
                .filter(|file| is_par_volume(&file.role))
                .count() as u32,
        };
        let download_wait = self.download_wait_by_job.get(&state.job_id);
        Some(JobInfo {
            job_id: state.job_id,
            job_hash: Some(state.job_hash),
            name: state.spec.name.clone(),
            error: if let JobStatus::Failed { error } = &state.status {
                Some(error.clone())
            } else if self.blocked_restores.contains_key(&state.job_id) {
                state.failure_error.clone()
            } else {
                None
            },
            download_wait_reason: download_wait
                .map(|wait| wait.reason)
                .or_else(|| self.job_dispatch_hold_reason(state.job_id))
                .map(str::to_owned),
            download_retry_at_epoch_ms: download_wait.and_then(|wait| wait.retry_at_epoch_ms),
            status,
            download_state,
            finalizing_download: self.jobs_finalizing_download.contains(&state.job_id),
            fetching_repair_data: self
                .graph_jobs
                .get(&state.job_id)
                .is_some_and(|graph| graph.fetching_repair_data()),
            post_state,
            run_state,
            progress: Self::effective_progress(state),
            total_bytes: total,
            downloaded_bytes: Self::effective_downloaded_bytes(state),
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            phase_progress: Vec::new(),
            failed_bytes: state.failed_bytes,
            health,
            terminal_discards: Vec::new(),
            total_files: self.blocked_restores.get(&state.job_id).map_or_else(
                || state.total_file_count() as u32,
                |request| request.spec.files.len() as u32,
            ),
            completed_files: if self.blocked_restores.contains_key(&state.job_id) {
                0
            } else {
                state.complete_file_count() as u32
            },
            file_remaining_bytes: self
                .graph_jobs
                .get(&state.job_id)
                .map(super::super::graph::job::GraphJob::remaining_bytes)
                .unwrap_or_default(),
            remaining_par_files,
            password: state.spec.password.clone(),
            category: state.spec.category.clone(),
            metadata: state.spec.metadata.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            server_attribution: state.server_attribution.contributions().to_vec(),
            created_at_epoch_ms: state.created_at_epoch_ms,
        })
    }

    // List all jobs: the live ones, then history.
    #[cfg(test)]
    pub(crate) fn list_jobs(&self) -> Vec<JobInfo> {
        self.report_all_for_test();
        self.shared_state.list_jobs()
    }
}
