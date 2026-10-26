// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::jobs::handle::{CancellationOrigin, JobInfo};
use crate::jobs::{JobId, JobUpdate};

mod admission;
mod counters;
mod history;
mod intents;
pub(crate) use admission::{AdmissionResources, PreparedAdd, RestartAdmission};
pub(crate) use history::clear_restart_artifacts;

/// The control plane's shared facts. A guard never leaves a synchronous
/// closure, so callers copy inputs out before doing asynchronous work.
pub struct Queue {
    inner: Mutex<QueueInner>,
    history: std::sync::RwLock<Arc<[JobInfo]>>,
    revision: watch::Sender<u64>,
    intent_wake: tokio::sync::Notify,
    pub(crate) resources: std::sync::OnceLock<AdmissionResources>,
    pub(crate) closed: std::sync::atomic::AtomicBool,
    ticker_started: std::sync::atomic::AtomicBool,
    pub(crate) counters: Arc<TransferCounters>,
    bandwidth_ledger: Mutex<crate::bandwidth::service::BandwidthLedgerRuntime>,
}

pub(crate) struct RuntimeGuard(pub Arc<Queue>);

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        self.0
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.0.write(|_| Change::changed(()));
    }
}

pub struct Change<T> {
    value: T,
    changed: bool,
}

impl<T> Change<T> {
    pub fn changed(value: T) -> Self {
        Self {
            value,
            changed: true,
        }
    }

    pub fn unchanged(value: T) -> Self {
        Self {
            value,
            changed: false,
        }
    }
}

#[derive(Default)]
pub struct QueueInner {
    pub globals: Globals,
    // Rows are added, removed and reordered only through `push_job`,
    // `retain_jobs` and `reindex_jobs`, which keep `job_positions` in step.
    pub jobs: Vec<JobRow>,
    // Each row's position in `jobs`, so a lookup by id is one probe.
    job_positions: HashMap<JobId, usize>,
    pub history_summary: crate::operations::metrics_store::JobStatusCounts,
    pub(crate) admissions: std::collections::HashSet<JobId>,
    pub(crate) history_operations: std::collections::HashSet<JobId>,
    pub(crate) pending_cleanup: std::collections::HashSet<JobId>,
    pub(crate) nntp: Option<(u64, weaver_nntp::client::NntpClient, usize)>,
    // The endpoint's request count and the pipeline's latest answer, tagged
    // with the request count it answers.
    pub(crate) diagnostics_requested: u64,
    pub(crate) diagnostics: Option<(u64, crate::pipeline::diagnostics::PipelineDiagnostics)>,
    pub(crate) metrics: Option<crate::operations::metrics::MetricsSnapshot>,
}

#[derive(Clone, Default)]
pub struct Globals {
    pub paused: bool,
    pub scheduled_pause: bool,
    pub speed_test_paused: bool,
    pub post_processing_paused: bool,
    pub schedule_replay_paused: bool,
    pub schedule_hold_reason: Option<String>,
    pub shutting_down: bool,
    pub speed_limit: u64,
    pub scheduled_speed_limit: Option<u64>,
    pub propagation_delay: std::time::Duration,
    pub max_concurrent_nzbs: Option<std::num::NonZeroU32>,
    pub hardware_profile: Option<crate::runtime::HardwareProfile>,
    pub hardware_profile_in_force: Option<crate::jobs::handle::HardwareProfileInForce>,
    pub scheduled_hardware_profile: Option<crate::runtime::HardwareProfile>,
    pub random_read_iops: Option<f64>,
    pub runtime_paths: Option<RuntimePaths>,
    pub nntp_generation: u64,
    pub nntp_active: Option<crate::jobs::handle::NntpRuntimeActivation>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub data: std::path::PathBuf,
    pub intermediate: std::path::PathBuf,
    pub complete: std::path::PathBuf,
}

pub struct JobRow {
    pub id: JobId,
    pub intents: Intents,
    pub report: Report,
    pub counters: Arc<JobCounters>,
    pub(crate) phases: HashMap<crate::jobs::phase_progress::JobPhase, counters::PhaseSample>,
    pub(crate) sampled_phases: Vec<crate::jobs::phase_progress::JobPhaseProgress>,
    // The download rate over `counters.downloaded`, advanced on every metrics
    // tick for as long as the row lives, on the global gauge's sample instants.
    pub(crate) download_rate: Option<crate::operations::metrics::RateSeries>,
    pub(crate) admission: Option<PreparedAdd>,
}

#[derive(Clone, Default)]
pub struct Intents {
    pub paused: Option<bool>,
    pub pause_seq: u64,
    pub cancelled: Option<CancellationOrigin>,
    pub delete_history_after_cancel: bool,
    pub update: Option<JobUpdate>,
    pub rename_to: Option<String>,
    pub restore: Option<crate::jobs::handle::RestoreJobRequest>,
    pub cancel_post_processing: bool,
    pub seq: u64,
}

pub struct Report {
    pub job: JobInfo,
    // Published only from the actor's durable admission ledger.
    pub nzb_admitted: bool,
    pub seen_intent_seq: u64,
    pub retiring: bool,
    pub restoring: bool,
    pub post_processing_cancellable: bool,
    pub moving: bool,
    pub download_active: bool,
    pub download_work_pending: bool,
    pub observed_byte_at_ms: u64,
    pub propagation_until_epoch_ms: Option<f64>,
}

#[derive(Default)]
pub(crate) struct TransferCounters {
    pub bandwidth: crate::bandwidth::service::BandwidthCounters,
    pub decoded: AtomicU64,
    pub articles: AtomicU64,
}

#[derive(Default)]
pub struct JobCounters {
    pub(crate) totals: std::sync::OnceLock<Arc<TransferCounters>>,
    pub downloaded: AtomicU64,
    pub decoded: AtomicU64,
    pub placed: AtomicU64,
    pub articles_done: AtomicU32,
    pub last_byte_at_ms: AtomicU64,
}

impl JobCounters {
    pub(crate) fn record_decoded(&self, bytes: u64) {
        self.decoded
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        if let Some(totals) = self.totals.get() {
            totals
                .decoded
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

impl JobRow {
    pub fn new(job: JobInfo) -> Self {
        Self {
            id: job.job_id,
            intents: Intents::default(),
            report: Report {
                nzb_admitted: false,
                download_active: job.download_state == crate::DownloadState::Downloading,
                download_work_pending: false,
                job,
                seen_intent_seq: 0,
                retiring: false,
                restoring: false,
                post_processing_cancellable: false,
                moving: false,
                observed_byte_at_ms: 0,
                propagation_until_epoch_ms: None,
            },
            counters: Arc::default(),
            phases: HashMap::new(),
            sampled_phases: Vec::new(),
            download_rate: None,
            admission: None,
        }
    }

    // A lane holds the job, or bytes arrived since the pipeline last reported it.
    pub(crate) fn download_activity(&self) -> bool {
        self.report.download_active
            || self
                .counters
                .last_byte_at_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                > self.report.observed_byte_at_ms
    }

    pub fn info(&self, globals: &Globals) -> JobInfo {
        let mut job = self.report.job.clone();
        let activity = self.download_activity();
        let propagation = self
            .report
            .propagation_until_epoch_ms
            .filter(|until| *until > crate::jobs::epoch_ms_now())
            .filter(|_| {
                matches!(
                    job.status,
                    crate::JobStatus::Queued | crate::JobStatus::Downloading
                )
            });
        if propagation.is_some() {
            job.download_wait_reason = Some(crate::jobs::handle::PROPAGATION_WAIT_REASON.into());
            job.download_retry_at_epoch_ms = propagation;
        }
        // A pause or resume the pipeline has not applied yet is already the
        // state the queue holds the job to.
        if self.intents.pause_seq > self.report.seen_intent_seq {
            if self.intents.paused == Some(true) {
                job.status = crate::JobStatus::Paused;
                job.run_state = crate::RunState::Paused;
            } else if self.intents.paused == Some(false) && job.status == crate::JobStatus::Paused {
                job.status = crate::JobStatus::Queued;
                job.run_state = crate::RunState::Active;
            }
        }
        let global_pause = globals.paused || globals.speed_test_paused;
        if global_pause || self.intents.paused == Some(true) || propagation.is_some() {
            if job.download_state == crate::DownloadState::Downloading {
                job.download_state = crate::DownloadState::Queued;
            }
            // The global pause holds every job whose download it stops.
            if global_pause
                && job.download_state == crate::DownloadState::Queued
                && job.post_state == crate::PostState::Idle
                && job.status != crate::JobStatus::AwaitingQueueScripts
            {
                job.run_state = crate::RunState::Paused;
            }
        } else if activity
            && matches!(
                job.download_state,
                crate::DownloadState::Queued | crate::DownloadState::Complete
            )
        {
            job.download_state = crate::DownloadState::Downloading;
        } else if !activity
            && job.download_state == crate::DownloadState::Downloading
            && self.report.download_work_pending
            && !job.finalizing_download
        {
            // Work is owed but no lane holds the job: it waits for lanes.
            job.download_state = crate::DownloadState::Queued;
        }
        if !self.phases.is_empty() {
            job.phase_progress = self.sampled_phases.clone();
        }
        if let Some(update) = &self.intents.update {
            intents::apply_update(&mut job, update);
        }
        job
    }
}

impl QueueInner {
    // Every row the queue holds. A cancelled job stays listed, as it was,
    // until its history row has committed and the done event drops the row.
    pub fn visible_jobs(&self) -> impl Iterator<Item = JobInfo> + '_ {
        self.jobs.iter().map(|row| row.info(&self.globals))
    }

    // The row for `id`, found through the position index.
    pub(crate) fn job(&self, id: JobId) -> Option<&JobRow> {
        let row = self.jobs.get(*self.job_positions.get(&id)?)?;
        debug_assert_eq!(row.id, id, "the queue's position index is out of step");
        (row.id == id).then_some(row)
    }

    pub(crate) fn push_job(&mut self, row: JobRow) {
        self.job_positions.insert(row.id, self.jobs.len());
        self.jobs.push(row);
    }

    pub(crate) fn retain_jobs(&mut self, keep: impl FnMut(&JobRow) -> bool) {
        self.jobs.retain(keep);
        self.reindex_jobs();
    }

    // Rebuilds the position index after `jobs` was replaced or reordered.
    pub(crate) fn reindex_jobs(&mut self) {
        self.job_positions = self
            .jobs
            .iter()
            .enumerate()
            .map(|(position, row)| (row.id, position))
            .collect();
    }
}

impl Queue {
    pub fn new(jobs: Vec<JobInfo>) -> Self {
        Self {
            history: std::sync::RwLock::default(),
            inner: Mutex::new({
                let mut inner = QueueInner {
                    jobs: jobs.into_iter().map(JobRow::new).collect(),
                    ..QueueInner::default()
                };
                inner.reindex_jobs();
                inner
            }),
            revision: watch::channel(0).0,
            intent_wake: tokio::sync::Notify::new(),
            resources: std::sync::OnceLock::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
            ticker_started: std::sync::atomic::AtomicBool::new(false),
            counters: Arc::default(),
            bandwidth_ledger: Mutex::default(),
        }
    }

    pub fn read<T>(&self, read: impl FnOnce(&QueueInner) -> T) -> T {
        read(&self.inner.lock().expect("queue lock poisoned"))
    }

    pub(crate) fn history(&self) -> Arc<[JobInfo]> {
        self.history.read().expect("history lock poisoned").clone()
    }

    pub(crate) fn job_counters(&self, id: JobId) -> Option<Arc<JobCounters>> {
        self.read(|inner| {
            inner
                .jobs
                .iter()
                .find(|row| row.id == id)
                .map(|row| row.counters.clone())
        })
    }

    // Called while changing queue membership, so retirement, summary and
    // the terminal event share one revision. History readers own this view.
    pub(crate) fn replace_history(&self, inner: &mut QueueInner, rows: Arc<[JobInfo]>) {
        inner.history_summary = crate::operations::metrics_store::JobStatusCounts::from_statuses(
            rows.iter().map(|job| &job.status),
        );
        *self.history.write().expect("history lock poisoned") = rows;
    }

    /// Mutate control-plane facts synchronously. An asynchronous closure cannot
    /// retain the queue guard while it waits.
    ///
    /// ```compile_fail
    /// use weaver_server_core::pipeline::queue::{Change, Queue};
    /// let queue = Queue::new(vec![]);
    /// queue.write(async |inner| {
    ///     tokio::task::yield_now().await;
    ///     inner.globals.paused = true;
    ///     Change::changed(())
    /// });
    /// ```
    pub fn write<T>(&self, write: impl FnOnce(&mut QueueInner) -> Change<T>) -> T {
        let mut inner = self.inner.lock().expect("queue lock poisoned");
        let change = write(&mut inner);
        if change.changed {
            // Serialize the watermark with the write: a subscriber must not
            // observe a later write with an earlier revision.
            self.revision
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        change.value
    }

    pub fn revision(&self) -> u64 {
        *self.revision.borrow()
    }

    pub fn write_intent<T>(&self, write: impl FnOnce(&mut QueueInner) -> Change<T>) -> T {
        let result = self.write(write);
        self.intent_wake.notify_one();
        result
    }

    pub(crate) async fn intent_changed(&self) {
        self.intent_wake.notified().await;
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn report_jobs(&self, live: Vec<JobInfo>) {
        self.write(|inner| {
            let ids: Vec<_> = live.iter().map(|job| job.job_id).collect();
            let mut reports: HashMap<_, _> =
                live.into_iter().map(|job| (job.job_id, job)).collect();
            for row in &mut inner.jobs {
                if let Some(job) = reports.remove(&row.id) {
                    row.report.job = job;
                }
            }
            for id in ids {
                if let Some(job) = reports.remove(&id) {
                    inner.push_job(JobRow::new(job));
                }
            }
            Change::changed(())
        });
    }
}

#[cfg(test)]
mod label_tests {
    use super::*;
    use crate::{DownloadState, JobStatus, PostState, RunState};

    fn downloading(id: u64) -> JobInfo {
        JobInfo {
            job_id: JobId(id),
            job_hash: None,
            name: format!("job-{id}"),
            status: JobStatus::Downloading,
            download_state: DownloadState::Downloading,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: PostState::Idle,
            run_state: RunState::Active,
            progress: 0.0,
            total_bytes: 100,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 1,
            completed_files: 0,
            remaining_par_files: 0,
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            server_attribution: Vec::new(),
            created_at_epoch_ms: 0.0,
        }
    }

    // A row the pipeline last reported with a lane holding it.
    fn active_row(id: u64) -> JobRow {
        let mut row = JobRow::new(downloading(id));
        row.report.download_active = true;
        row.report.download_work_pending = true;
        row
    }

    #[test]
    fn queue_global_pause_reports_every_held_download_as_paused() {
        let globals = Globals {
            paused: true,
            ..Globals::default()
        };
        for row in [active_row(1), JobRow::new(downloading(2))] {
            let job = row.info(&globals);
            assert_eq!(job.run_state, RunState::Paused);
            assert_eq!(job.download_state, DownloadState::Queued);
        }

        let speed_test = Globals {
            speed_test_paused: true,
            ..Globals::default()
        };
        assert_eq!(active_row(3).info(&speed_test).run_state, RunState::Paused);

        // Post-processing is not a download the global pause stops.
        let mut extracting = downloading(4);
        extracting.status = JobStatus::Extracting;
        extracting.download_state = DownloadState::Complete;
        extracting.post_state = PostState::Extracting;
        assert_eq!(
            JobRow::new(extracting).info(&globals).run_state,
            RunState::Active
        );
        assert_eq!(
            active_row(5).info(&Globals::default()).run_state,
            RunState::Active
        );
    }

    #[test]
    fn queue_job_holding_no_lane_while_work_is_owed_reads_queued() {
        let mut row = active_row(1);
        row.report.download_active = false;
        let job = row.info(&Globals::default());
        assert_eq!(job.download_state, DownloadState::Queued);
        assert_eq!(job.run_state, RunState::Active);

        // A lane holds it: Downloading.
        assert_eq!(
            active_row(2).info(&Globals::default()).download_state,
            DownloadState::Downloading
        );

        // Payload in and the pipeline draining: not a wait for lanes.
        let mut finalizing = active_row(3);
        finalizing.report.download_active = false;
        finalizing.report.job.finalizing_download = true;
        assert_eq!(
            finalizing.info(&Globals::default()).download_state,
            DownloadState::Downloading
        );
    }

    #[test]
    fn queue_job_waiting_for_lanes_keeps_the_dispatcher_wait_reason() {
        let mut row = active_row(1);
        row.report.download_active = false;
        row.report.job.download_wait_reason = Some("checkpoint".into());
        let job = row.info(&Globals::default());
        assert_eq!(job.download_state, DownloadState::Queued);
        assert_eq!(job.download_wait_reason.as_deref(), Some("checkpoint"));
    }

    #[test]
    fn queue_pause_then_resume_report_the_intent_before_the_pipeline_applies_it() {
        let queue = Queue::new(vec![downloading(1)]);
        queue.write(|inner| {
            inner.jobs[0].report.download_active = true;
            inner.jobs[0].report.download_work_pending = true;
            Change::changed(())
        });
        let view = |queue: &Queue| queue.read(|inner| inner.jobs[0].info(&inner.globals));

        queue.set_job_paused(JobId(1), true).unwrap();
        let paused = view(&queue);
        assert_eq!(paused.status, JobStatus::Paused);
        assert_eq!(paused.run_state, RunState::Paused);
        assert_eq!(paused.download_state, DownloadState::Queued);

        // The pipeline applies the pause and reports it.
        queue.write(|inner| {
            let row = &mut inner.jobs[0];
            row.report.seen_intent_seq = row.intents.seq;
            row.report.job.status = JobStatus::Paused;
            row.report.job.download_state = DownloadState::Queued;
            row.report.job.run_state = RunState::Paused;
            row.report.download_active = false;
            Change::changed(())
        });
        assert_eq!(view(&queue).run_state, RunState::Paused);

        queue.set_job_paused(JobId(1), false).unwrap();
        let resumed = view(&queue);
        assert_eq!(resumed.status, JobStatus::Queued);
        assert_eq!(resumed.run_state, RunState::Active);
        assert_eq!(resumed.download_state, DownloadState::Queued);

        // A resume under the global pause is still held by it.
        let held = queue.read(|inner| {
            let globals = Globals {
                paused: true,
                ..inner.globals.clone()
            };
            inner.jobs[0].info(&globals)
        });
        assert_eq!(held.run_state, RunState::Paused);
    }
}
