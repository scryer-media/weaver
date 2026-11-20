// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::jobs::class::{ByProtocol, OwnedRows, PIPELINE};
use crate::jobs::handle::{CancellationOrigin, JobInfo};
use crate::jobs::{JobId, JobUpdate, Protocol};

mod admission;
#[cfg(feature = "pipeline-test-fixtures")]
#[doc(hidden)]
pub mod units {
    pub mod admission {
        pub use super::super::admission::*;
    }
}
mod counters;
mod history;
mod intents;
pub(crate) mod order;
pub mod paging;
pub use admission::destination_roots;
pub use admission::staging_dir_for;
pub(crate) use admission::{
    AdmissionResources, PreparedAdd, RestartAdmission, destination_root, make_staging_dir,
    outline_read, staging_dir_on,
};
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use counters::COUNTER_SAMPLES;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use history::FAILED_SWEEP_PAGE;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use history::SweepPlaces;
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use history::between_sweeps;
pub(crate) use history::{MoveOut, clear_left_working_dirs, clear_restart_artifacts};

/// The control plane's shared facts. A guard never leaves a synchronous
/// closure, so callers copy inputs out before doing asynchronous work.
pub struct Queue {
    inner: Mutex<QueueInner>,
    history: std::sync::RwLock<Arc<[JobInfo]>>,
    revision: watch::Sender<u64>,
    intent_wake: tokio::sync::Notify,
    // The actor's word on whether anything is there for a periodic driver
    // to move. The counters ticker waits on it while it says no.
    drive: watch::Sender<bool>,
    pub resources: std::sync::OnceLock<AdmissionResources>,
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
    // Every change to a row goes through `RowMut` (`job_mut`, `row_mut_at`),
    // never `iter_mut`, so the count of jobs with work waiting stays right.
    pub jobs: Vec<JobRow>,
    // The jobs each other class publishes beside the pipeline's, in that
    // class's order, each with its position, so asking whose a job is, or
    // reading one, costs one probe per class and no walk.
    owned: ByProtocol<OwnedRows>,
    // Each row's position in `jobs`, so a lookup by id is one probe.
    job_positions: HashMap<JobId, usize>,
    // The rows in a phase, the only ones the metrics tick samples.
    phased_jobs: std::collections::HashSet<JobId>,
    // How many rows have work waiting (see [`JobRow::work_waits`]), moved
    // by `push_job`, `remove_job`, `reindex_jobs` and every [`RowMut`].
    pub waiting: usize,
    pub history_summary: crate::operations::metrics_store::JobStatusCounts,
    pub(crate) admissions: std::collections::HashSet<JobId>,
    pub(crate) history_operations: std::collections::HashSet<JobId>,
    // The failed jobs the housekeeping sweep read as due and has not yet
    // held. An admission of one takes it out, so the sweep never acts on a
    // reading older than a retry. Empty except while a page is swept.
    pub(crate) sweep_listed: std::collections::HashSet<JobId>,
    // The failed jobs whose files a sweep could not remove and has warned
    // about in this run. Each later sweep still tries, and says so only at
    // debug; a sweep that removes them, an admission and a history delete
    // take them out. Bounded by the failed jobs whose files are stuck.
    pub(crate) sweep_warned: std::collections::HashSet<JobId>,
    pub pending_cleanup: PendingCleanup,
    // The jobs a move of a working folder is out for, counted: the move to
    // the intermediate folder set now, and the removal of a folder such a
    // move claimed for a job that left. Nothing else removes or makes the
    // job's folders until its count is gone: see `MoveOut`.
    pub(crate) moves_out: HashMap<JobId, usize>,
    // How often a cleanup wait found a move out for the job it waits for.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) waits_on_moves: usize,
    // How many restarts went on to clear the job's folders.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) restarts_clearing: usize,
    pub(crate) nntp: Option<(u64, weaver_nntp::client::NntpClient, usize)>,
    // What WireGuard tunnels asked usenet to give back, for the pipeline to
    // answer on its next turn. Drained by every turn.
    pub(crate) socket_give_backs: Vec<crate::jobs::TunnelGiveBack>,
    // The endpoint's request count and the pipeline's latest answer, tagged
    // with the request count it answers.
    pub(crate) diagnostics_requested: u64,
    pub(crate) diagnostics: Option<(u64, crate::pipeline::diagnostics::PipelineDiagnostics)>,
    pub(crate) metrics: Option<weaver_server_base::operations::metrics::MetricsSnapshot>,
    // Bumped by every intent write, so the actor can tell a turn with no new
    // intent from one that needs a walk over the rows.
    intent_epoch: u64,
}

// The jobs whose leavings are still being cleared, each held by one or more
// of: the cancel's cleanup of its folders, a move of its working folder
// still out, a removal of a staging folder made for it after it left. Its id
// is added, restarted or deleted from history again only once none holds it.
#[derive(Default)]
pub struct PendingCleanup(HashMap<JobId, u32>);

// What holds a job in [`PendingCleanup`]. A job has at most one cancel's
// cleanup and one move out, each a bit; each disowned read that made a
// staging folder adds a removal, counted from `Staging` up.
#[derive(Clone, Copy, PartialEq)]
pub enum CleanupHolder {
    Folders = 1,
    Move = 2,
    Staging = 4,
}

impl PendingCleanup {
    pub fn contains(&self, id: &JobId) -> bool {
        self.0.contains_key(id)
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn folders_held(&self, id: &JobId) -> bool {
        self.0
            .get(id)
            .is_some_and(|held| held & CleanupHolder::Folders as u32 != 0)
    }

    pub fn hold(&mut self, id: JobId, holder: CleanupHolder) {
        let held = self.0.entry(id).or_default();
        if holder == CleanupHolder::Staging {
            *held += holder as u32;
        } else {
            *held |= holder as u32;
        }
    }

    // `holder` is done; true when that let the job go.
    pub fn done(&mut self, id: JobId, holder: CleanupHolder) -> bool {
        let Some(held) = self.0.get_mut(&id) else {
            return false;
        };
        if holder != CleanupHolder::Staging {
            *held &= !(holder as u32);
        } else if *held >= holder as u32 {
            *held -= holder as u32;
        }
        let free = *held == 0;
        if free {
            self.0.remove(&id);
        }
        free
    }
}

// Counts the actor's walks over every queue row, so a test can pin how many
// a pass makes.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(crate) static ROW_WALKS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn count_walk() {
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    ROW_WALKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[derive(Clone, Default)]
pub struct Globals {
    // The holds in force, the operator's, the schedule's and a speed test's.
    pub holds: crate::jobs::HoldSet,
    pub post_processing_paused: bool,
    pub schedule_replay_paused: bool,
    pub schedule_hold_reason: Option<String>,
    pub shutting_down: bool,
    // The operator's global bandwidth limits.
    pub bandwidth: crate::proxies::Bandwidth,
    // The global limits a schedule rule has in force, if one does.
    pub scheduled_bandwidth: Option<crate::proxies::Bandwidth>,
    // The split a schedule rule has in force over the operator's, if one does.
    pub scheduled_split: Option<crate::bandwidth::BandwidthSplit>,
    pub propagation_delay: std::time::Duration,
    pub max_concurrent_nzbs: Option<std::num::NonZeroU32>,
    pub hardware_profile: Option<crate::runtime::HardwareProfile>,
    pub hardware_profile_in_force: Option<crate::jobs::handle::HardwareProfileInForce>,
    pub scheduled_hardware_profile: Option<crate::runtime::HardwareProfile>,
    pub random_read_iops: Option<f64>,
    pub storage_classification: Option<(
        weaver_server_base::runtime::system_profile::StorageClass,
        weaver_server_base::runtime::system_profile::FilesystemType,
    )>,
    pub runtime_paths: Option<RuntimePaths>,
    pub nntp_generation: u64,
    pub nntp_active: Option<crate::jobs::handle::NntpRuntimeActivation>,
}

impl Globals {
    // Whether a hold stops the queue's downloads, whoever holds it.
    pub fn downloads_held(&self) -> bool {
        self.holds.stops(crate::jobs::HoldOwner::All, false)
    }

    // Whether anything over the whole queue stops every download: a hold, a
    // schedule replay, or shutdown.
    pub(crate) fn holds_downloads(&self) -> bool {
        self.downloads_held() || self.schedule_replay_paused || self.shutting_down
    }
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
    pub sampled_phases: Vec<crate::jobs::phase_progress::JobPhaseProgress>,
    // The download rate over `counters.downloaded`, advanced on every metrics
    // tick for as long as the row lives, on the global gauge's sample instants.
    pub(crate) download_rate: Option<weaver_server_base::operations::metrics::RateSeries>,
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
    // The status a retiring job ended with, kept off `job` until history
    // holds the job: no reader sees it finished while history would refuse
    // to remove it. The pipeline's own record of its verdict.
    pub(crate) finished_status: Option<crate::JobStatus>,
    pub restoring: bool,
    pub post_processing_cancellable: bool,
    pub moving: bool,
    pub download_active: bool,
    pub download_work_pending: bool,
    pub observed_byte_at_ms: u64,
    pub propagation_until_epoch_ms: Option<f64>,
    // A restart found a checkpoint of the job: some of its bytes are on
    // disk, though nothing of it is installed to say how many.
    pub(crate) on_disk: bool,
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
    // What a lane's press for a full route waits on. A press stands while
    // the job has download work still to hand out, since only that work can
    // want the dial the route refused. Each refused dial a lane reports takes
    // the next `lane_refusals_sent`; the pipeline counts each report it hears
    // in `lane_refusals_heard`; a dispatch pass that leaves the job nothing
    // queued and no paced retry stores the reports it had heard by then in
    // `drained_through`, and any other pass stores 0. A press is over once
    // `drained_through` reaches its own report: its work was heard of, and a
    // later pass found nothing left for a dial (the work went to another
    // lane, or the job moved past its download).
    pub(crate) lane_refusals_sent: AtomicU64,
    pub(crate) lane_refusals_heard: AtomicU64,
    pub(crate) drained_through: AtomicU64,
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
                finished_status: None,
                restoring: false,
                post_processing_cancellable: false,
                moving: false,
                observed_byte_at_ms: 0,
                propagation_until_epoch_ms: None,
                on_disk: false,
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

    // Whether the status [`Self::info`] gives the row is queued, downloading
    // or checking, read from the few fields that status is made from: the
    // reported status and a pause or resume the pipeline has not applied.
    // No global enters it.
    pub(crate) fn work_waits(&self) -> bool {
        let status = &self.report.job.status;
        if self.intents.pause_seq > self.report.seen_intent_seq {
            match self.intents.paused {
                Some(true) => return false,
                Some(false) if *status == crate::JobStatus::Paused => return true,
                _ => {}
            }
        }
        matches!(
            status,
            crate::JobStatus::Queued | crate::JobStatus::Downloading | crate::JobStatus::Checking
        )
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
        let global_pause = globals.downloads_held();
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

    pub(crate) fn intent_epoch(&self) -> u64 {
        self.intent_epoch
    }

    // The row for `id`, found through the position index.
    pub(crate) fn job(&self, id: JobId) -> Option<&JobRow> {
        self.jobs.get(self.job_position(id)?)
    }

    // The row for `id` to change, found through the position index.
    pub fn job_mut(&mut self, id: JobId) -> Option<RowMut<'_>> {
        let position = self.job_position(id)?;
        Some(self.row_mut_at(position))
    }

    // The row at `position` to change. Every change to a row that can move
    // its status goes through this guard, which keeps the count of rows
    // with work waiting in step as it is let go.
    pub fn row_mut_at(&mut self, position: usize) -> RowMut<'_> {
        self.row_mut_with_globals(position).0
    }

    // The same, with the globals to read beside it.
    pub(crate) fn row_mut_with_globals(&mut self, position: usize) -> (RowMut<'_>, &Globals) {
        let row = &mut self.jobs[position];
        (
            RowMut {
                waited: row.work_waits(),
                row,
                waiting: &mut self.waiting,
            },
            &self.globals,
        )
    }

    /// Whether any job has work waiting: its status is queued, downloading
    /// or checking. Answered from a count kept as rows change, not by a
    /// walk of the rows.
    pub fn work_waiting(&self) -> bool {
        debug_assert_eq!(
            self.waiting,
            self.jobs
                .iter()
                .filter(|row| {
                    matches!(
                        row.view(&self.globals).status(),
                        crate::JobStatus::Queued
                            | crate::JobStatus::Downloading
                            | crate::JobStatus::Checking
                    )
                })
                .count(),
            "the count of jobs with work waiting is out of step with the rows"
        );
        self.waiting > 0
    }

    pub(crate) fn contains_job(&self, id: JobId) -> bool {
        self.job_position(id).is_some()
    }

    // Where `id` stands in `jobs`, found through the position index.
    pub fn job_position(&self, id: JobId) -> Option<usize> {
        let position = *self.job_positions.get(&id)?;
        let row = self.jobs.get(position)?;
        debug_assert_eq!(row.id, id, "the queue's position index is out of step");
        (row.id == id).then_some(position)
    }

    // The rows in a phase.
    pub(crate) fn phased_jobs(&self) -> impl Iterator<Item = JobId> + '_ {
        self.phased_jobs.iter().copied()
    }

    pub fn push_job(&mut self, row: JobRow) {
        if !row.phases.is_empty() {
            self.phased_jobs.insert(row.id);
        }
        self.job_positions.insert(row.id, self.jobs.len());
        self.waiting += usize::from(row.work_waits());
        self.jobs.push(row);
    }

    // Removes one row, moving up only the rows after it in the index.
    pub fn remove_job(&mut self, id: JobId) -> Option<JobRow> {
        let position = self.job_position(id)?;
        self.job_positions.remove(&id);
        self.phased_jobs.remove(&id);
        let row = self.jobs.remove(position);
        self.waiting -= usize::from(row.work_waits());
        for (offset, later) in self.jobs[position..].iter().enumerate() {
            self.job_positions.insert(later.id, position + offset);
        }
        Some(row)
    }

    pub fn retain_jobs(&mut self, keep: impl FnMut(&JobRow) -> bool) {
        self.jobs.retain(keep);
        self.reindex_jobs();
    }

    /// The jobs `protocol`'s class publishes beside the pipeline's, in its
    /// order.
    pub fn owned(&self, protocol: Protocol) -> &[JobInfo] {
        self.owned[protocol].rows()
    }

    /// The rows `protocol`'s class publishes, to change through their own
    /// methods.
    pub(crate) fn owned_mut(&mut self, protocol: Protocol) -> &mut OwnedRows {
        &mut self.owned[protocol]
    }

    /// The job `job_id` another class published, if it is one: one probe per
    /// class.
    pub fn owned_job(&self, job_id: JobId) -> Option<&JobInfo> {
        self.owned.iter().find_map(|(_, owned)| owned.get(job_id))
    }

    /// The class that answers for `job_id`: the one that published it, or
    /// else the pipeline. One probe per class.
    pub fn class_of(&self, job_id: JobId) -> Protocol {
        self.owned
            .iter()
            .find(|(_, owned)| owned.position(job_id).is_some())
            .map_or(PIPELINE, |(protocol, _)| protocol)
    }

    /// Every class's published jobs in one order, by when each was added,
    /// each class's own order kept; a tie goes to the class first in the
    /// protocol table. Each class's place in its rows is given with the row.
    pub fn owned_rows(&self) -> impl Iterator<Item = (Protocol, usize, &JobInfo)> {
        let mut next: ByProtocol<usize> = ByProtocol::default();
        std::iter::from_fn(move || {
            let (protocol, at, job) = self
                .owned
                .iter()
                .filter_map(|(protocol, owned)| {
                    let at = next[protocol];
                    owned.rows().get(at).map(|job| (protocol, at, job))
                })
                .reduce(|first, other| {
                    if other.2.created_at_epoch_ms < first.2.created_at_epoch_ms {
                        other
                    } else {
                        first
                    }
                })?;
            next[protocol] += 1;
            Some((protocol, at, job))
        })
    }

    // Rebuilds the position index after `jobs` was replaced or reordered.
    pub(crate) fn reindex_jobs(&mut self) {
        self.job_positions = self
            .jobs
            .iter()
            .enumerate()
            .map(|(position, row)| (row.id, position))
            .collect();
        self.phased_jobs = self
            .jobs
            .iter()
            .filter(|row| !row.phases.is_empty())
            .map(|row| row.id)
            .collect();
        // This walk already visits every row, so the count is taken again
        // in it: rows may have been dropped.
        self.waiting = self.jobs.iter().filter(|row| row.work_waits()).count();
    }
}

// A queue row borrowed to change. Letting it go moves the queue's count of
// rows with work waiting by what the change did to this row: one compare.
pub struct RowMut<'a> {
    row: &'a mut JobRow,
    waiting: &'a mut usize,
    waited: bool,
}

impl std::ops::Deref for RowMut<'_> {
    type Target = JobRow;

    fn deref(&self) -> &JobRow {
        self.row
    }
}

impl std::ops::DerefMut for RowMut<'_> {
    fn deref_mut(&mut self) -> &mut JobRow {
        self.row
    }
}

impl Drop for RowMut<'_> {
    fn drop(&mut self) {
        match (self.waited, self.row.work_waits()) {
            (false, true) => *self.waiting += 1,
            (true, false) => *self.waiting -= 1,
            _ => {}
        }
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
            drive: watch::channel(false).0,
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

    pub fn history(&self) -> Arc<[JobInfo]> {
        self.history.read().expect("history lock poisoned").clone()
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn job_counters(&self, id: JobId) -> Option<Arc<JobCounters>> {
        self.read(|inner| inner.job(id).map(|row| row.counters.clone()))
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
    ///     inner.globals.post_processing_paused = true;
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

    // One holder of a job's pending cleanup is done: one write, a change
    // only when that let the job go, which wakes whoever waits for it.
    pub(crate) fn release_cleanup(&self, id: JobId, holder: CleanupHolder) {
        self.write(|inner| {
            if inner.pending_cleanup.done(id, holder) {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
    }

    // The job's marked staging folders under every destination root, as a
    // restart clears them: the job's own path under each root, no listing.
    pub(crate) async fn remove_job_staging(&self, id: JobId) {
        let Some(resources) = self.resources.get() else {
            return;
        };
        if let Err(error) = history::remove_job_staging(&resources.staging_roots().await, id).await
        {
            tracing::warn!(job_id = id.0, %error, "could not clear the staging directories of a cancelled job");
        }
    }

    pub fn revision(&self) -> u64 {
        *self.revision.borrow()
    }

    pub fn write_intent<T>(&self, write: impl FnOnce(&mut QueueInner) -> Change<T>) -> T {
        let result = self.write(|inner| {
            inner.intent_epoch = inner.intent_epoch.wrapping_add(1);
            write(inner)
        });
        self.intent_wake.notify_one();
        result
    }

    pub(crate) async fn intent_changed(&self) {
        self.intent_wake.notified().await;
    }

    // Set by the actor at the end of each turn; a repeat of the same answer
    // wakes no one.
    pub fn set_driving(&self, driving: bool) {
        self.drive.send_if_modified(|current| {
            let changed = *current != driving;
            *current = driving;
            changed
        });
    }

    // The actor's last answer: whether it has work to drive.
    pub(crate) fn driving(&self) -> bool {
        *self.drive.borrow()
    }

    pub(crate) fn subscribe_drive(&self) -> watch::Receiver<bool> {
        self.drive.subscribe()
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn report_jobs(&self, live: Vec<JobInfo>) {
        self.write(|inner| {
            let ids: Vec<_> = live.iter().map(|job| job.job_id).collect();
            let mut reports: HashMap<_, _> =
                live.into_iter().map(|job| (job.job_id, job)).collect();
            for position in 0..inner.jobs.len() {
                if let Some(job) = reports.remove(&inner.jobs[position].id) {
                    inner.row_mut_at(position).report.job = job;
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
