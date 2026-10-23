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
    pub jobs: Vec<JobRow>,
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
        if globals.paused
            || globals.speed_test_paused
            || self.intents.paused == Some(true)
            || propagation.is_some()
        {
            if job.download_state == crate::DownloadState::Downloading {
                job.download_state = crate::DownloadState::Queued;
            }
        } else if activity
            && matches!(
                job.download_state,
                crate::DownloadState::Queued | crate::DownloadState::Complete
            )
        {
            job.download_state = crate::DownloadState::Downloading;
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
    pub fn visible_jobs(&self) -> impl Iterator<Item = JobInfo> + '_ {
        self.jobs
            .iter()
            .filter(|row| row.intents.cancelled.is_none())
            .map(|row| row.info(&self.globals))
    }
}

impl Queue {
    pub fn new(jobs: Vec<JobInfo>) -> Self {
        Self {
            history: std::sync::RwLock::default(),
            inner: Mutex::new(QueueInner {
                jobs: jobs.into_iter().map(JobRow::new).collect(),
                ..QueueInner::default()
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
                    inner.jobs.push(JobRow::new(job));
                }
            }
            Change::changed(())
        });
    }
}
