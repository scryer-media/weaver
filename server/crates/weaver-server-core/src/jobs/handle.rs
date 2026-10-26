// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;

use crate::events::model::PipelineEvent;
use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::record::ActiveFileIdentity;

use crate::jobs::error::SchedulerError;
use crate::jobs::model::{JobSpec, JobStatus, JobUpdate};
use crate::operations::metrics::{MetricsSnapshot, PipelineMetrics};
use crate::pipeline::queue::{Change, Queue};

pub const FINISHED_JOBS_RUNTIME_CAP: usize = 1_000;
pub const PROPAGATION_WAIT_REASON: &str = "propagation_delay";

type JobCancellationCallback = Arc<dyn Fn() + Send + Sync>;

// Signals active job work without waiting for the single-threaded scheduler
// loop. This lets a user cancellation interrupt a repair that currently owns
// that loop before the queued cancellation command is handled.
#[derive(Clone, Default)]
struct JobCancellationRegistry {
    state: Arc<Mutex<JobCancellationState>>,
}

#[derive(Default)]
struct JobCancellationState {
    callbacks: HashMap<JobId, Vec<JobCancellationCallback>>,
    requested: HashSet<JobId>,
}

impl JobCancellationRegistry {
    fn register(&self, job_id: JobId, callback: JobCancellationCallback) {
        let cancel_now = {
            let mut state = self
                .state
                .lock()
                .expect("job cancellation registry poisoned");
            let cancel_now = state.requested.contains(&job_id);
            state
                .callbacks
                .entry(job_id)
                .or_default()
                .push(Arc::clone(&callback));
            cancel_now
        };
        if cancel_now {
            callback();
        }
    }

    fn cancel(&self, job_id: JobId) -> bool {
        let callbacks = {
            let mut state = self
                .state
                .lock()
                .expect("job cancellation registry poisoned");
            state.requested.insert(job_id);
            state.callbacks.get(&job_id).cloned().unwrap_or_default()
        };
        let cancelled = !callbacks.is_empty();
        for callback in callbacks {
            callback();
        }
        cancelled
    }

    fn requested(&self, job_id: JobId) -> bool {
        self.state
            .lock()
            .expect("job cancellation registry poisoned")
            .requested
            .contains(&job_id)
    }

    fn clear(&self, job_id: JobId) {
        let mut state = self
            .state
            .lock()
            .expect("job cancellation registry poisoned");
        state.callbacks.remove(&job_id);
        state.requested.remove(&job_id);
    }
}

#[derive(Clone)]
pub struct SharedPipelineState {
    pub(crate) queue: Arc<Queue>,
    // The owner of the jobs published beside the pipeline's.
    owner: Arc<RwLock<Option<Arc<dyn QueueOwner>>>>,
    // The holds in force, as the queue records them, for every owner of
    // queue items to follow.
    holds: tokio::sync::watch::Sender<HoldSet>,
    // The torrent side of the global download limit, once torrents run.
    torrent_rate: Arc<RwLock<Option<Arc<dyn TorrentRate>>>>,
    // How the global download limit is shared out, as last put in force.
    bandwidth_split: Arc<RwLock<crate::bandwidth::SplitState>>,
    metrics: Arc<PipelineMetrics>,
    download_block: Arc<RwLock<DownloadBlockState>>,
    server_quota_blocked: Arc<AtomicBool>,
    // The egress whose download quota holds work back, while one does.
    egress_quota_block: Arc<RwLock<Option<EgressQuotaBlock>>>,
    proxy_runtime: Arc<RwLock<Option<Arc<crate::proxies::ProxyRuntime>>>>,
    server_transfer_policy:
        Arc<RwLock<Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>>>>,
    nntp_pool: Arc<RwLock<Option<Arc<weaver_nntp::pool::NntpPool>>>>,
    // Per-server BODY transport state from the download lanes, indexed by
    // pool server index. Published on the tuning tick, not per response.
    download_transport: Arc<RwLock<Vec<ServerTransportHealth>>>,
    // First-byte latency the last connection test measured, keyed by saved
    // server id. The download lanes read it once, to pick a starting depth
    // for a server they have never fetched from; every later depth decision
    // comes from their own measurements.
    server_probe_latency: Arc<RwLock<HashMap<u32, Duration>>>,
    // Memory one conventional 7z extraction may hold, as the hardware
    // profile in force decides it. Read when an extraction is admitted, so a
    // profile put in force now reaches the next extraction. `u64::MAX` means no profile cap, and
    // the extraction ceiling alone bounds the decoder.
    sevenz_decode_memory_bytes: Arc<AtomicU64>,
    // How a direct-unpack chase widens its 7z decoder, as the hardware
    // profile in force decides it. Read when a chase starts, so a profile put
    // in force now reaches the next chase.
    chase_widening: Arc<RwLock<crate::runtime::hardware_profile::ChaseWidening>>,
    job_cancellations: JobCancellationRegistry,
    // The per-article stream (`ArticleDownloaded`, `SegmentDecoded`, ...),
    // kept off the job-level broadcast: a download emits several of these
    // per article, and every always-on job-level subscriber (event
    // persistence, the queue replay producer) was woken for each one only to
    // discard it. Nothing is even built here unless someone is listening.
    segment_events: broadcast::Sender<PipelineEvent>,
    // The speed test service, held weakly: the service holds this handle.
    speed_test: Arc<std::sync::Mutex<std::sync::Weak<crate::speed_test::ServiceInner>>>,
}

// Bounded like the job-level broadcast; a lagging listener drops articles,
// never the pipeline.
const SEGMENT_EVENT_CAPACITY: usize = 1024;

impl SharedPipelineState {
    pub fn new(metrics: Arc<PipelineMetrics>, initial_jobs: Vec<JobInfo>) -> Self {
        let metrics_snapshot = metrics.raw_snapshot();
        let queue = Arc::new(Queue::new(initial_jobs));
        queue.write(|inner| {
            inner.metrics = Some(metrics_snapshot);
            Change::unchanged(())
        });
        let (segment_events, _) = broadcast::channel(SEGMENT_EVENT_CAPACITY);
        Self {
            queue,
            owner: Arc::new(RwLock::new(None)),
            holds: tokio::sync::watch::channel(HoldSet::default()).0,
            torrent_rate: Arc::new(RwLock::new(None)),
            bandwidth_split: Arc::new(RwLock::new(crate::bandwidth::SplitState::default())),
            metrics,
            download_block: Arc::new(RwLock::new(DownloadBlockState::default())),
            server_quota_blocked: Arc::new(AtomicBool::new(false)),
            egress_quota_block: Arc::new(RwLock::new(None)),
            proxy_runtime: Arc::new(RwLock::new(None)),
            server_transfer_policy: Arc::new(RwLock::new(None)),
            nntp_pool: Arc::new(RwLock::new(None)),
            download_transport: Arc::new(RwLock::new(Vec::new())),
            server_probe_latency: Arc::new(RwLock::new(HashMap::new())),
            sevenz_decode_memory_bytes: Arc::new(AtomicU64::new(u64::MAX)),
            chase_widening: Arc::new(RwLock::new(
                crate::runtime::hardware_profile::ChaseWidening::default(),
            )),
            job_cancellations: JobCancellationRegistry::default(),
            segment_events,
            speed_test: Arc::new(std::sync::Mutex::new(std::sync::Weak::new())),
        }
    }

    // Subscribe to the per-article event stream. Job-level events stay on
    // [`SchedulerHandle::subscribe_events`].
    pub fn subscribe_segment_events(&self) -> broadcast::Receiver<PipelineEvent> {
        self.segment_events.subscribe()
    }

    // Publish one per-article event, building it only if a listener exists.
    pub fn publish_segment_event(&self, event: impl FnOnce() -> PipelineEvent) {
        if self.segment_events.receiver_count() > 0 {
            let _ = self.segment_events.send(event());
        }
    }

    // --- Reader methods (called by API handlers) ---

    // The queue in one order: by when each job was added, each owner's own
    // order kept among its jobs, then the finished jobs.
    pub fn list_jobs(&self) -> Vec<JobInfo> {
        self.queue.read(|inner| {
            let mut live = inner.visible_jobs().peekable();
            let mut owned = inner.torrents.iter().cloned().peekable();
            std::iter::from_fn(|| match (live.peek(), owned.peek()) {
                (Some(job), Some(other)) if other.created_at_epoch_ms < job.created_at_epoch_ms => {
                    owned.next()
                }
                (Some(_), _) => live.next(),
                (None, _) => owned.next(),
            })
            .chain(self.queue.history().iter().cloned())
            .collect()
        })
    }

    // How many published jobs are in each status, counted under the read
    // lock without copying a job.
    pub fn job_status_counts(&self) -> crate::operations::metrics_store::JobStatusCounts {
        self.queue.read(|inner| {
            inner.history_summary.with_statuses(
                inner
                    .jobs
                    .iter()
                    .map(|row| &row.report.job.status)
                    .chain(inner.torrents.iter().map(|job| &job.status)),
            )
        })
    }

    // The revision of the published job list; it moves on every publish.
    pub fn job_revision(&self) -> u64 {
        self.queue.revision()
    }

    // Live download rate of every transferring job, read off the published
    // job list in place.
    //
    // Off the pipeline entirely: it takes the read side of the lock the
    // orchestrator already writes on its 100 ms tick and copies nothing but
    // `(job id, rate)` pairs, so a metrics subscriber sampling it every
    // 250 ms costs less than the job-list clone the queue readers make.
    pub fn job_download_rates(&self) -> Vec<(JobId, u64)> {
        self.queue.read(|inner| {
            let mut rates: Vec<_> = inner
                .jobs
                .iter()
                .filter(|row| row.intents.cancelled.is_none())
                .filter_map(|row| {
                    let phases = if row.phases.is_empty() {
                        &row.report.job.phase_progress
                    } else {
                        &row.sampled_phases
                    };
                    phases
                        .iter()
                        .find(|phase| {
                            phase.phase == crate::jobs::phase_progress::JobPhase::Downloading
                        })
                        .and_then(|phase| phase.rate_bps)
                        .map(|rate| (row.id, rate))
                })
                .collect();
            rates.extend(job_download_rates(&inner.torrents));
            rates
        })
    }

    pub fn get_job(&self, job_id: JobId) -> Option<JobInfo> {
        self.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .find(|row| row.id == job_id)
                .map(|row| row.info(&inner.globals))
                .or_else(|| {
                    inner
                        .torrents
                        .iter()
                        .find(|job| job.job_id == job_id)
                        .cloned()
                })
                .or_else(|| {
                    self.queue
                        .history()
                        .iter()
                        .find(|job| job.job_id == job_id)
                        .cloned()
                })
        })
    }

    // Includes hidden cancellations and terminal rows awaiting durable retirement.
    pub fn queued_job_ids(&self) -> HashSet<JobId> {
        self.queue
            .read(|inner| inner.jobs.iter().map(|row| row.id).collect())
    }

    // Whether the queue as a whole is held.
    pub fn is_paused(&self) -> bool {
        self.holds.borrow().stops(HoldOwner::All, false)
    }

    pub fn holds(&self) -> HoldSet {
        self.holds.borrow().clone()
    }

    // Wakes on each change of the holds.
    pub fn subscribe_holds(&self) -> tokio::sync::watch::Receiver<HoldSet> {
        self.holds.subscribe()
    }

    pub(crate) fn schedule_replay_paused(&self) -> bool {
        self.queue
            .read(|inner| inner.globals.schedule_replay_paused)
    }

    pub fn is_post_processing_paused(&self) -> bool {
        self.queue
            .read(|inner| inner.globals.post_processing_paused)
    }

    pub fn set_post_processing_paused(&self, paused: bool) {
        self.queue.write_intent(|inner| {
            inner.globals.post_processing_paused = paused;
            Change::changed(())
        });
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.queue
            .read(|inner| inner.metrics.clone())
            .unwrap_or_else(|| self.metrics.raw_snapshot())
    }

    pub fn raw_metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.raw_snapshot()
    }

    pub(crate) fn register_job_cancellation(
        &self,
        job_id: JobId,
        callback: JobCancellationCallback,
    ) {
        self.job_cancellations.register(job_id, callback);
    }

    pub(crate) fn cancel_active_job_work(&self, job_id: JobId) -> bool {
        self.job_cancellations.cancel(job_id)
    }

    pub(crate) fn is_job_cancellation_requested(&self, job_id: JobId) -> bool {
        self.job_cancellations.requested(job_id)
    }

    pub(crate) fn clear_job_cancellations(&self, job_id: JobId) {
        self.job_cancellations.clear(job_id);
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    pub fn download_block(&self) -> DownloadBlockState {
        let mut state = self.download_block.read().unwrap().clone();
        state.schedule_hold_reason = self
            .queue
            .read(|inner| inner.globals.schedule_hold_reason.clone());
        if state.schedule_hold_reason.is_some() && state.kind == DownloadBlockKind::None {
            state.kind = DownloadBlockKind::Scheduled;
        }
        state
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn publish_jobs(&self, jobs: Vec<JobInfo>) {
        self.queue.write(|inner| {
            inner.jobs = jobs
                .into_iter()
                .map(crate::pipeline::queue::JobRow::new)
                .collect();
            self.queue.replace_history(inner, Arc::from(Vec::new()));
            crate::pipeline::queue::Change::changed(())
        });
    }

    pub fn load_history(&self, history: Vec<JobInfo>) {
        self.queue.write(|inner| {
            self.queue.replace_history(inner, history.into());
            Change::changed(())
        });
    }

    // Publish the torrents, kept beside whatever the pipeline publishes, and
    // say what the publish changed as the pipeline says it of its own jobs.
    // The owner publishes through its handle, which carries the events.
    fn publish_torrent_jobs(&self, torrents: Vec<JobInfo>) -> Vec<PipelineEvent> {
        self.queue.write(|inner| {
            let events = owned_job_events(&inner.torrents, &torrents);
            inner.torrents = torrents;
            Change::changed(events)
        })
    }

    // Seat the owner of the jobs published beside the pipeline's.
    pub fn seat_owner(&self, owner: Arc<dyn QueueOwner>) {
        *self.owner.write().unwrap() = Some(owner);
    }

    // The owner that answers for `job_id`; `None` is the pipeline.
    fn owner_of(&self, job_id: JobId) -> Option<Arc<dyn QueueOwner>> {
        let owned = self
            .queue
            .read(|inner| inner.torrents.iter().any(|job| job.job_id == job_id));
        owned.then(|| self.owner.read().unwrap().clone()).flatten()
    }

    pub fn subscribe_job_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.queue.subscribe()
    }

    // Advance the rate windows and publish the metrics snapshot. Returns
    // whether any published rate is still above zero, so the caller keeps
    // sampling quickly until the gauges have settled.
    #[cfg(any(test, feature = "test-support"))]
    pub fn refresh_metrics_snapshot(&self, now: std::time::Instant) -> bool {
        // The gauge integrates the transfer total, the counter the queue tick
        // hands it, so a test tick and a live tick feed one series.
        let snapshot = self
            .metrics
            .sample_totals(now, self.queue.counters.bandwidth.total(), 0, 0);
        let rates_moving = snapshot.current_download_speed > 0
            || snapshot.articles_per_sec > 0.0
            || snapshot.decode_rate_mbps > 0.0;
        self.queue.write(|inner| {
            inner.metrics = Some(snapshot);
            Change::unchanged(())
        });
        rates_moving
    }

    // Book wire bytes the way a lane and its completion do: the transfer total
    // the download gauge integrates, and the lifetime downloaded counter.
    #[cfg(any(test, feature = "test-support"))]
    pub fn record_wire_bytes(&self, bytes: u64) {
        self.queue.counters.bandwidth.record(bytes);
        self.metrics
            .bytes_downloaded
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    // Put a hold in force, as one change of the holds.
    pub fn hold(&self, hold: Hold) {
        self.change_holds(|holds| holds.hold(hold));
    }

    // Lift what `origin` holds over `owner`, as one change of the holds.
    pub fn release(&self, origin: HoldOrigin, owner: HoldOwner) {
        self.change_holds(|holds| holds.release(origin, owner));
    }

    // Put back the holds kept across a restart.
    pub fn restore_holds(&self, kept: HoldSet) {
        self.change_holds(|holds| *holds = kept);
    }

    // The queue records the holds, and the pipeline puts them in force at
    // its next turn. The watch moves under the same lock, so every owner
    // that follows it sees the holds in the order the queue took them.
    fn change_holds(&self, change: impl FnOnce(&mut HoldSet)) {
        self.queue.write_intent(|inner| {
            let before = inner.globals.holds.clone();
            change(&mut inner.globals.holds);
            if inner.globals.holds == before {
                return Change::unchanged(());
            }
            self.holds.send_replace(inner.globals.holds.clone());
            Change::changed(())
        });
    }

    // Seat the torrent side of the global download limit.
    pub fn seat_torrent_rate(&self, rate: Arc<dyn TorrentRate>) {
        *self.torrent_rate.write().unwrap() = Some(rate);
    }

    pub fn torrent_rate(&self) -> Option<Arc<dyn TorrentRate>> {
        self.torrent_rate.read().unwrap().clone()
    }

    pub fn set_bandwidth_split(&self, state: crate::bandwidth::SplitState) {
        *self.bandwidth_split.write().unwrap() = state;
    }

    pub fn bandwidth_split(&self) -> crate::bandwidth::SplitState {
        *self.bandwidth_split.read().unwrap()
    }

    pub fn set_download_block(&self, mut state: DownloadBlockState) {
        let mut current = self.download_block.write().unwrap();
        self.apply_quota_blocks(&mut state);
        *current = state;
    }

    // Lay the quota blocks over a pause-derived block state. A manual or
    // scheduled pause outranks both; an egress quota outranks a server quota,
    // since no server can be reached past it.
    fn apply_quota_blocks(&self, state: &mut DownloadBlockState) {
        let egress = self.egress_quota_block.read().unwrap().clone();
        let quota_kind = matches!(
            state.kind,
            DownloadBlockKind::EgressQuota | DownloadBlockKind::ServerQuota
        );
        if quota_kind {
            state.kind = if state.scheduled_speed_limit.is_some() {
                DownloadBlockKind::Scheduled
            } else {
                DownloadBlockKind::None
            };
        }
        let overridable = matches!(
            state.kind,
            DownloadBlockKind::None | DownloadBlockKind::Scheduled
        );
        state.egress_id = None;
        state.egress_name = None;
        state.used_bytes = 0;
        state.limit_bytes = 0;
        state.remaining_bytes = 0;
        state.window_starts_at_epoch_ms = None;
        state.window_ends_at_epoch_ms = None;
        if let Some(block) = egress {
            state.egress_id = Some(block.egress_id);
            state.egress_name = Some(block.egress_name);
            state.used_bytes = block.used_bytes;
            state.limit_bytes = block.limit_bytes;
            state.remaining_bytes = block.remaining_bytes;
            state.window_starts_at_epoch_ms = block.window_starts_at_epoch_ms;
            state.window_ends_at_epoch_ms = block.window_ends_at_epoch_ms;
            state.timezone_name = block.timezone_name;
            if overridable {
                state.kind = DownloadBlockKind::EgressQuota;
            }
        } else if overridable && self.server_quota_blocked.load(Ordering::Relaxed) {
            state.kind = DownloadBlockKind::ServerQuota;
        }
    }

    pub fn egress_quota_block(&self) -> Option<EgressQuotaBlock> {
        self.egress_quota_block.read().unwrap().clone()
    }

    // Record which egress quota, if any, holds work back, and re-lay the
    // published block state over it.
    pub fn set_egress_quota_block(&self, block: Option<EgressQuotaBlock>) {
        let mut state = self.download_block.write().unwrap();
        {
            let mut current = self.egress_quota_block.write().unwrap();
            if *current == block {
                return;
            }
            *current = block;
        }
        self.apply_quota_blocks(&mut state);
    }

    // Memory one conventional 7z extraction may hold. `u64::MAX` when no
    // profile has capped it.
    pub fn sevenz_decode_memory_bytes(&self) -> u64 {
        self.sevenz_decode_memory_bytes.load(Ordering::Relaxed)
    }

    // Apply the chosen profile's 7z decoder allowance. Extractions already
    // running keep the allowance they were admitted with.
    pub fn set_sevenz_decode_memory_bytes(&self, bytes: u64) {
        self.sevenz_decode_memory_bytes
            .store(bytes.max(1), Ordering::Relaxed);
    }

    // How a direct-unpack chase widens its 7z decoder. The balanced
    // profile's widening until a profile is put in force.
    pub fn chase_widening(&self) -> crate::runtime::hardware_profile::ChaseWidening {
        *self.chase_widening.read().unwrap()
    }

    // Apply the chosen profile's chase widening. Chases already running keep
    // the widening they started with.
    pub fn set_chase_widening(&self, widening: crate::runtime::hardware_profile::ChaseWidening) {
        *self.chase_widening.write().unwrap() = widening;
    }

    // The hardware profile in force, or `None` before a pipeline has
    // resolved one.
    pub fn hardware_profile_in_force(&self) -> Option<HardwareProfileInForce> {
        self.queue
            .read(|inner| inner.globals.hardware_profile_in_force)
    }

    // Publish the hardware profile the pipeline has put in force.
    pub fn set_hardware_profile_in_force(&self, in_force: HardwareProfileInForce) {
        self.queue.write(|inner| {
            if inner.globals.hardware_profile_in_force == Some(in_force) {
                return Change::unchanged(());
            }
            inner.globals.hardware_profile_in_force = Some(in_force);
            Change::changed(())
        });
    }

    pub fn server_quota_blocked(&self) -> bool {
        self.server_quota_blocked.load(Ordering::Relaxed)
    }

    pub fn set_server_quota_blocked(&self, blocked: bool) {
        let mut state = self.download_block.write().unwrap();
        self.server_quota_blocked.store(blocked, Ordering::Relaxed);
        self.apply_quota_blocks(&mut state);
    }

    pub fn set_server_transfer_policy(
        &self,
        registry: Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
    ) {
        *self.server_transfer_policy.write().unwrap() = Some(registry);
    }

    pub fn server_transfer_policy(
        &self,
    ) -> Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>> {
        self.server_transfer_policy.read().unwrap().clone()
    }

    pub fn proxy_runtime(&self) -> Option<Arc<crate::proxies::ProxyRuntime>> {
        self.proxy_runtime.read().expect("proxy runtime").clone()
    }

    pub fn set_nntp_pool(&self, pool: Arc<weaver_nntp::pool::NntpPool>) {
        *self.nntp_pool.write().unwrap() = Some(pool);
    }

    pub fn nntp_pool(&self) -> Option<Arc<weaver_nntp::pool::NntpPool>> {
        self.nntp_pool.read().unwrap().clone()
    }

    pub fn set_nntp_runtime_activation(&self, activation: NntpRuntimeActivation) {
        self.queue.write(|inner| {
            inner.globals.nntp_active = Some(activation);
            Change::changed(())
        });
    }

    pub fn nntp_runtime_activation(&self) -> Option<NntpRuntimeActivation> {
        self.queue.read(|inner| inner.globals.nntp_active)
    }

    pub fn set_download_transport_health(&self, health: Vec<ServerTransportHealth>) {
        *self.download_transport.write().unwrap() = health;
    }

    pub fn download_transport_health(&self) -> Vec<ServerTransportHealth> {
        self.download_transport.read().unwrap().clone()
    }

    // Record what a connection test measured, so a server the lanes have
    // never fetched from can start at a sensible pipelining depth.
    pub fn note_server_probe_latency(&self, server_id: u32, latency: Duration) {
        self.server_probe_latency
            .write()
            .unwrap()
            .insert(server_id, latency);
    }

    pub fn server_probe_latency(&self, server_id: u32) -> Option<Duration> {
        self.server_probe_latency
            .read()
            .unwrap()
            .get(&server_id)
            .copied()
    }
}

// What the download lanes have learned about one server's BODY transport.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerTransportHealth {
    // Pool server index this describes.
    pub server_idx: usize,
    // Command-to-status-line wait.
    pub latency_ms: Option<f64>,
    // Status-line-to-terminator wait for one article.
    pub transfer_ms: Option<f64>,
    // "good", "moderate" or "slow"; absent until a latency is measured.
    pub latency_band: Option<String>,
    // BODY pipelining depth in use, where 1 means sequential.
    pub pipeline_depth: u32,
    // Set once a server has proved twice that it cannot pipeline cleanly.
    pub pinned_sequential: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadBlockKind {
    None,
    ManualPause,
    SpeedTest,
    Scheduled,
    EgressQuota,
    ServerQuota,
}

impl DownloadBlockKind {
    // Every variant, in gate-reason label order. The Prometheus exporter
    // renders one series per variant; deriving the label set from `ALL` keeps
    // a new gate reason from silently vanishing off `/metrics`.
    pub const ALL: [Self; 6] = [
        Self::None,
        Self::ManualPause,
        Self::SpeedTest,
        Self::Scheduled,
        Self::EgressQuota,
        Self::ServerQuota,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ManualPause => "manual_pause",
            Self::SpeedTest => "speed_test",
            Self::Scheduled => "scheduled",
            Self::EgressQuota => "egress_quota",
            Self::ServerQuota => "server_quota",
        }
    }
}

// Where a manual queue reorder should land a job. The order only breaks ties
// within a dispatch priority band: a LOW job moved to the top still yields to
// HIGH/NORMAL work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueMoveTarget {
    Top,
    Bottom,
    Offset(i64),
}

// What the queue asks of one job, whoever owns it.
#[derive(Debug, Clone)]
pub enum QueueAction {
    Pause,
    Resume,
    Cancel(CancellationOrigin),
    Update(JobUpdate),
    Move(QueueMoveTarget),
}

// The owner of the jobs published beside the pipeline's. Each job id has one
// owner: the one that published it, or else the pipeline.
#[async_trait::async_trait]
pub trait QueueOwner: Send + Sync {
    async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), SchedulerError>;
}

// Who put a hold in force, in the order their reasons take precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldOrigin {
    Manual,
    Scheduled,
    SpeedTest,
}

impl HoldOrigin {
    // Whether a pause or resume from this origin replaces or lifts a hold
    // from `other`. The operator and the schedule share one pause, each
    // overriding the other's; a speed test's hold is its own.
    const fn shares(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::SpeedTest, Self::SpeedTest)
                | (
                    Self::Manual | Self::Scheduled,
                    Self::Manual | Self::Scheduled
                )
        )
    }

    // The reason an item a hold from this origin stops gives.
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Manual => "paused",
            Self::Scheduled => "scheduled",
            Self::SpeedTest => "speed test",
        }
    }
}

// What a hold stops of the items it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldScope {
    // Downloading stops; what is complete keeps seeding.
    Downloads,
    // Everything stops, seeding too.
    Everything,
}

// Which items a hold covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HoldOwner {
    // Every item in the queue.
    All,
    // The torrents alone.
    Torrents,
}

impl HoldOwner {
    // The setting an operator's hold over these items is kept in.
    pub const fn setting(self) -> &'static str {
        match self {
            Self::All => "global_paused",
            Self::Torrents => "torrents_paused",
        }
    }
}

// One hold on the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hold {
    pub origin: HoldOrigin,
    pub scope: HoldScope,
    pub owner: HoldOwner,
}

impl Hold {
    // The operator's hold over `owner`: the queue's pause leaves what is
    // complete seeding, the torrents' pause stops them whole.
    pub const fn manual(owner: HoldOwner) -> Self {
        Self {
            origin: HoldOrigin::Manual,
            scope: match owner {
                HoldOwner::All => HoldScope::Downloads,
                HoldOwner::Torrents => HoldScope::Everything,
            },
            owner,
        }
    }

    // Whether the hold covers `target` and what it stops of it, before
    // what the target says of the schedule.
    const fn reaches(self, target: HoldTarget) -> bool {
        let covers = matches!(
            (self.owner, target.owner),
            (HoldOwner::All, _) | (HoldOwner::Torrents, HoldOwner::Torrents)
        );
        covers
            && match self.scope {
                HoldScope::Downloads => !target.complete,
                HoldScope::Everything => true,
            }
    }

    // Whether the hold stops `target`. The schedule's holds leave a target
    // that runs through them.
    const fn stops(self, target: HoldTarget) -> bool {
        self.reaches(target)
            && !(target.runs_through_schedule && matches!(self.origin, HoldOrigin::Scheduled))
    }
}

// An item a hold is asked about: whose it is, whether it has finished
// downloading, and whether it runs through the schedule's holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoldTarget {
    pub owner: HoldOwner,
    pub complete: bool,
    pub runs_through_schedule: bool,
}

impl HoldTarget {
    // An item of `owner` that every hold over it may stop.
    pub const fn of(owner: HoldOwner, complete: bool) -> Self {
        Self {
            owner,
            complete,
            runs_through_schedule: false,
        }
    }
}

// The holds in force. Every owner of queue items follows them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HoldSet(Vec<Hold>);

impl HoldSet {
    // Put `hold` in force in place of any its origin shares over the same
    // items.
    pub fn hold(&mut self, hold: Hold) {
        self.release(hold.origin, hold.owner);
        self.0.push(hold);
        self.0.sort_unstable();
    }

    // Lift what `origin` shares of the holds over `owner`.
    pub fn release(&mut self, origin: HoldOrigin, owner: HoldOwner) {
        self.0
            .retain(|hold| hold.owner != owner || !origin.shares(hold.origin));
    }

    // Whether any hold stops an item of `owner`, given whether it has
    // finished downloading.
    pub fn stops(&self, owner: HoldOwner, complete: bool) -> bool {
        self.reason(owner, complete).is_some()
    }

    // Who stops that item, by precedence, while anyone does.
    pub fn reason(&self, owner: HoldOwner, complete: bool) -> Option<HoldOrigin> {
        self.reason_for(HoldTarget::of(owner, complete))
    }

    // Who stops `target`, by precedence, while anyone does.
    pub fn reason_for(&self, target: HoldTarget) -> Option<HoldOrigin> {
        self.0
            .iter()
            .filter(|hold| hold.stops(target))
            .map(|hold| hold.origin)
            .min()
    }

    // Whether `target` runs only because it runs through the schedule's
    // holds: one of them reaches it and no hold stops it.
    pub fn spares(&self, target: HoldTarget) -> bool {
        self.reason_for(target).is_none() && self.0.iter().any(|hold| hold.reaches(target))
    }

    // Whether `origin` holds the items of `owner`.
    pub fn held_by(&self, origin: HoldOrigin, owner: HoldOwner) -> bool {
        self.0
            .iter()
            .any(|hold| hold.origin == origin && hold.owner == owner)
    }
}

// The torrent side of the global limits.
pub trait TorrentRate: Send + Sync {
    // What torrents downloaded at over the last second, in bytes per second.
    fn download_bps(&self) -> u64;
    // Put the torrents' download and upload rates in force.
    fn set_rates(&self, rates: crate::bandwidth::TorrentRates);
}

// Splice `job_id` to its new position in the manual queue order. Pure so the
// clamp/splice arithmetic is unit-testable apart from pipeline state.
pub fn splice_job_order(
    order: &mut Vec<JobId>,
    job_id: JobId,
    target: QueueMoveTarget,
) -> Result<bool, SchedulerError> {
    let Some(current) = order.iter().position(|id| *id == job_id) else {
        return Err(SchedulerError::JobNotFound(job_id));
    };
    let last = order.len() - 1;
    let new_index = match target {
        QueueMoveTarget::Top => 0,
        QueueMoveTarget::Bottom => last,
        QueueMoveTarget::Offset(delta) => {
            (current as i64).saturating_add(delta).clamp(0, last as i64) as usize
        }
    };
    if new_index == current {
        return Ok(false);
    }
    let id = order.remove(current);
    order.insert(new_index, id);
    Ok(true)
}

#[cfg(test)]
mod splice_job_order_tests {
    use super::*;

    fn order(ids: &[u64]) -> Vec<JobId> {
        ids.iter().copied().map(JobId).collect()
    }

    #[test]
    fn splices_top_bottom_and_clamped_offsets() {
        let mut jobs = order(&[1, 2, 3, 4]);
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Top).unwrap());
        assert_eq!(jobs, order(&[3, 1, 2, 4]));

        assert!(splice_job_order(&mut jobs, JobId(1), QueueMoveTarget::Bottom).unwrap());
        assert_eq!(jobs, order(&[3, 2, 4, 1]));

        assert!(splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Offset(-1)).unwrap());
        assert_eq!(jobs, order(&[3, 4, 2, 1]));

        // Offsets clamp at the edges instead of erroring.
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(-5)).is_ok());
        assert_eq!(jobs, order(&[3, 4, 2, 1]));
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(99)).unwrap());
        assert_eq!(jobs, order(&[4, 2, 1, 3]));

        // No-op moves report false; unknown ids error.
        assert!(!splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Top).unwrap());
        assert!(matches!(
            splice_job_order(&mut jobs, JobId(9), QueueMoveTarget::Top),
            Err(SchedulerError::JobNotFound(JobId(9)))
        ));
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DownloadBlockState {
    pub kind: DownloadBlockKind,
    // The egress whose download quota is reached, while one is. The byte
    // and window fields below describe that egress's quota.
    #[serde(default)]
    pub egress_id: Option<u32>,
    #[serde(default)]
    pub egress_name: Option<String>,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
    // Download limit imposed by the active schedule.
    #[serde(default)]
    pub scheduled_speed_limit: Option<u64>,
    // Why a schedule rule is holding new downloads, while one is.
    #[serde(default)]
    pub schedule_hold_reason: Option<String>,
}

// An egress download quota that holds work back: no leg of the route the
// work needs has quota left.
#[derive(Debug, Clone, PartialEq)]
pub struct EgressQuotaBlock {
    pub egress_id: u32,
    pub egress_name: String,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
}

impl Default for DownloadBlockState {
    fn default() -> Self {
        Self {
            kind: DownloadBlockKind::None,
            egress_id: None,
            egress_name: None,
            used_bytes: 0,
            limit_bytes: 0,
            remaining_bytes: 0,
            window_starts_at_epoch_ms: None,
            window_ends_at_epoch_ms: None,
            timezone_name: chrono::Local::now().offset().to_string(),
            scheduled_speed_limit: None,
            schedule_hold_reason: None,
        }
    }
}

// Commands sent to the scheduler's main loop.
#[derive(Clone)]
pub struct RestoreJobRequest {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    pub complete_files: HashSet<NzbFileId>,
    pub file_progress: HashMap<u32, crate::jobs::assembly::FileCoverage>,
    pub detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    pub file_identities: HashMap<u32, ActiveFileIdentity>,
    pub extracted_members: HashSet<String>,
    pub status: JobStatus,
    pub download_state: Option<crate::jobs::model::DownloadState>,
    pub post_state: Option<crate::jobs::model::PostState>,
    pub run_state: Option<crate::jobs::model::RunState>,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<JobStatus>,
    pub paused_resume_download_state: Option<crate::jobs::model::DownloadState>,
    pub paused_resume_post_state: Option<crate::jobs::model::PostState>,
    pub working_dir: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AddJobOptions {
    pub initially_paused: bool,
    // SCORE admission generation that must still be authoritative when the
    // scheduler durably materializes this job.
    pub semantic_materialization_generation: Option<i64>,
    // Promotion-lease generation that owns this candidate materialization.
    // Unlike admission generations, this must also still hold an unexpired
    // durable promotion lease when the active job row is inserted.
    pub semantic_promotion_generation: Option<i64>,
    // Archive passwords supplied beside the job's own (`Integration`, then
    // `DnzbHeader`), persisted with it so a restart still offers them: they are
    // not in the NZB bytes.
    pub supplied_password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
}

// Internal provenance for cancellation. Semantic cancellation is intentionally
// narrower than a user cancel: it may interrupt only download-lane work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOrigin {
    User,
    UserDuplicate,
    // A user cancel that leaves the job's files on disk.
    UserKeepingFiles,
    SemanticBad,
    SemanticSuperseded,
}

impl CancellationOrigin {
    pub fn is_user(self) -> bool {
        matches!(
            self,
            Self::User | Self::UserDuplicate | Self::UserKeepingFiles
        )
    }
}

// Per-id acceptance: `Ok` means the cancellation intent was recorded and
// hidden from the live queue; teardown and durable archival follow later.
pub type CancelJobsOutcome = BTreeMap<JobId, Result<(), SchedulerError>>;

// Which hardware profile's limits the pipeline is running under, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareProfileInForce {
    // The profile whose limits the next download, decode and extraction use.
    pub active: crate::runtime::HardwareProfile,
    // The profile a schedule rule put in force over the operator's choice.
    // `None` when no rule does, or when the rule's profile is one this
    // machine cannot honour and the choice stands instead.
    pub scheduled: Option<crate::runtime::HardwareProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NntpRuntimeActivation {
    pub generation: u64,
    pub configured_connections: usize,
}

// What deleting one history record left behind.
//
// The record itself is always gone when this is returned. A working
// directory whose ownership marker names the job but no longer matches the
// directory is not removed — nothing proves it is still the job's — and is
// listed here so the caller can say so.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryDeleteOutcome {
    pub left_in_place: Vec<std::path::PathBuf>,
}

// How many history ids one delete command carries. A batch is one
// transaction, one statement per table, and one reply the caller waits for,
// while the queue scripts of each job in it are stopped one after another
// before its directory goes. So a batch is also what one database error or
// one slow script fails whole: small enough to keep that to a few dozen
// rows, large enough that the transaction, lock and sweep a batch pays once
// are still shared across many.
pub const HISTORY_DELETE_BATCH_SIZE: usize = 50;

// What one batched history delete did, id by id. A refused id never holds
// back the rest of the batch.
#[derive(Debug, Default)]
pub struct HistoryDeleteBatchOutcome {
    // Every id whose record is gone, with what its cleanup did: what it left
    // on disk, or why it did not finish. A cleanup that failed still leaves
    // the record deleted, so the id is here either way and is announced as
    // removed; the error is reported beside that, never instead of it.
    pub deleted: BTreeMap<JobId, Result<HistoryDeleteOutcome, SchedulerError>>,
    // Ids whose record was not deleted, and why.
    pub refused: BTreeMap<JobId, SchedulerError>,
}

// One `(job, bytes per second)` pair per job whose download phase currently
// reports a rate. The rate is the value the independent ticker published for
// that job on the metrics tick, so it is the same estimator and the same
// instant as the global speed gauge.
pub fn job_download_rates(jobs: &[JobInfo]) -> Vec<(JobId, u64)> {
    jobs.iter()
        .filter_map(|job| {
            job.phase_progress
                .iter()
                .find(|phase| phase.phase == crate::jobs::phase_progress::JobPhase::Downloading)
                .and_then(|phase| phase.rate_bps)
                .map(|rate_bps| (job.job_id, rate_bps))
        })
        .collect()
}

// The job events that take an owner's published jobs from `before` to
// `after`: a job new to them is created, a job whose figures moved is paused,
// resumed, or otherwise updated.
fn owned_job_events(before: &[JobInfo], after: &[JobInfo]) -> Vec<PipelineEvent> {
    let before: HashMap<JobId, &JobInfo> = before.iter().map(|job| (job.job_id, job)).collect();
    after
        .iter()
        .filter_map(|job| {
            let job_id = job.job_id;
            let Some(was) = before.get(&job_id) else {
                return Some(PipelineEvent::JobCreated {
                    job_id,
                    name: job.name.clone(),
                    total_files: job.total_files,
                    total_bytes: job.total_bytes,
                });
            };
            let paused = |job: &JobInfo| job.status == JobStatus::Paused;
            match (paused(was), paused(job)) {
                _ if *was == job => None,
                (false, true) => Some(PipelineEvent::JobPaused { job_id }),
                (true, false) => Some(PipelineEvent::JobResumed { job_id }),
                _ => Some(PipelineEvent::PhaseProgressUpdated { job_id }),
            }
        })
        .collect()
}

impl JobInfo {
    /// The event that says how a finished job ended, as the queue hears it.
    /// None while the job has not ended.
    pub fn ended_event(&self) -> Option<PipelineEvent> {
        let job_id = self.job_id;
        match &self.status {
            JobStatus::Complete => Some(PipelineEvent::JobCompleted { job_id }),
            JobStatus::Failed { error } => Some(PipelineEvent::JobFailed {
                job_id,
                error: error.clone(),
            }),
            _ => None,
        }
    }
}

// Summary info about a job (returned by queries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: JobId,
    #[serde(default)]
    pub protocol: crate::jobs::model::Protocol,
    pub job_hash: Option<[u8; 32]>,
    pub name: String,
    pub status: JobStatus,
    pub download_state: crate::jobs::model::DownloadState,
    // Network download is complete, but decode/write or PAR2 discovery work remains.
    #[serde(default)]
    pub finalizing_download: bool,
    // Weaver is fetching the minimum PAR2 metadata or recovery data needed to finish.
    #[serde(default)]
    pub fetching_repair_data: bool,
    pub post_state: crate::jobs::model::PostState,
    pub run_state: crate::jobs::model::RunState,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    #[serde(default)]
    pub phase_progress: Vec<crate::jobs::phase_progress::JobPhaseProgress>,
    // Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    // Job health 0-1000 (1000 = perfect). Drops as articles fail.
    //
    // For a job that reached a terminal status this is the settled figure —
    // what the claim census concluded was delivered — not the live wire
    // counter, which knows nothing of the repairs and discards that answered
    // its misses.
    pub health: u32,
    // Files the settlement dropped from the delivery, with the bytes each one
    // took out of the accounting. Empty for every job that discarded nothing.
    #[serde(default)]
    pub terminal_discards: Vec<crate::jobs::model::TerminalDiscard>,
    // Total files in the NZB (all roles).
    #[serde(default)]
    pub total_files: u32,
    // Files fully downloaded and committed.
    #[serde(default)]
    pub completed_files: u32,
    // PAR2 recovery volumes not yet fetched (fetched on demand).
    #[serde(default)]
    pub remaining_par_files: u32,
    pub password: Option<String>,
    // Optional category (e.g. "tv", "movies").
    pub category: Option<String>,
    // Arbitrary key-value metadata from the submitting client.
    pub metadata: Vec<(String, String)>,
    // Output directory where extracted files land.
    pub output_dir: Option<String>,
    // Which servers served this job's articles, and how much each carried.
    //
    // Reporting only, and only as far as attribution reached: articles
    // Weaver could not name a server for are absent, so these counts are a
    // floor. Empty for a job downloaded before this was recorded.
    #[serde(default)]
    pub server_attribution: Vec<crate::jobs::server_attribution::JobServerContribution>,
    // Error message (only set when status is Failed).
    pub error: Option<String>,
    #[serde(default)]
    pub download_wait_reason: Option<String>,
    #[serde(default)]
    pub download_retry_at_epoch_ms: Option<f64>,
    // Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
}

// Handle for sending commands to the scheduler.
// Cloneable, can be shared across tasks.
#[derive(Clone)]
pub struct SchedulerHandle {
    event_tx: broadcast::Sender<PipelineEvent>,
    state: SharedPipelineState,
}

impl SchedulerHandle {
    // Attach the control-plane handle and its independent metrics ticker.
    pub fn new(event_tx: broadcast::Sender<PipelineEvent>, state: SharedPipelineState) -> Self {
        state
            .queue
            .start_ticker(state.metrics.clone(), event_tx.clone());
        Self { event_tx, state }
    }

    // Submit a new download job.
    pub async fn add_job(
        &self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
    ) -> Result<(), SchedulerError> {
        self.add_job_with_options(job_id, spec, nzb_path, nzb_zstd, AddJobOptions::default())
            .await
    }

    // Submit a new download job with explicit scheduler options.
    pub async fn add_job_with_options(
        &self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: AddJobOptions,
    ) -> Result<(), SchedulerError> {
        self.state
            .queue
            .clone()
            .add(
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                options,
                self.event_tx.clone(),
            )
            .await
    }

    // Restore a job from crash-recovery journal.
    pub async fn restore_job(&self, request: RestoreJobRequest) -> Result<(), SchedulerError> {
        self.state.queue.restore(request)
    }

    // Every queue action on one job comes through here, and the job's owner
    // answers it.
    async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), SchedulerError> {
        if let Some(owner) = self.state.owner_of(job_id) {
            return owner.act(job_id, action).await;
        }
        match action {
            QueueAction::Pause => self.state.queue.set_job_paused(job_id, true),
            QueueAction::Resume => self.state.queue.set_job_paused(job_id, false),
            QueueAction::Cancel(origin) => self
                .state
                .queue
                .cancel_jobs(BTreeSet::from([job_id]), origin)
                .remove(&job_id)
                .unwrap_or(Err(SchedulerError::JobNotFound(job_id))),
            QueueAction::Update(update) => self.state.queue.update_job(job_id, update),
            QueueAction::Move(target) => self.state.queue.reorder_jobs(vec![(job_id, target)]),
        }
    }

    // Pause a job.
    pub async fn pause_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.act(job_id, QueueAction::Pause).await
    }

    // Resume a paused job.
    pub async fn resume_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.act(job_id, QueueAction::Resume).await
    }

    // Cancel a job.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::User)
            .await
    }

    pub async fn cancel_duplicate_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::UserDuplicate)
            .await
    }

    pub async fn cancel_job_and_delete_history(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.state
            .queue
            .cancel_jobs_with_history_policy(
                BTreeSet::from([job_id]),
                CancellationOrigin::User,
                true,
            )
            .remove(&job_id)
            .unwrap_or(Err(SchedulerError::JobNotFound(job_id)))
    }

    // Cancel a job and keep whatever it has written so far.
    pub async fn cancel_job_keeping_files(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::UserKeepingFiles)
            .await
    }

    pub async fn cancel_semantic_bad(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::SemanticBad)
            .await
    }

    pub async fn cancel_semantic_superseded(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::SemanticSuperseded)
            .await
    }

    async fn cancel_job_with_origin(
        &self,
        job_id: JobId,
        origin: CancellationOrigin,
    ) -> Result<(), SchedulerError> {
        self.cancel_jobs(BTreeSet::from([job_id]), origin)
            .await?
            .remove(&job_id)
            .unwrap_or(Err(SchedulerError::JobNotFound(job_id)))
    }

    // Record cancellation and hide accepted rows immediately. Teardown and
    // durable history follow at the pipeline's next boundary. Another
    // owner's jobs go to it.
    pub async fn cancel_jobs(
        &self,
        job_ids: BTreeSet<JobId>,
        origin: CancellationOrigin,
    ) -> Result<CancelJobsOutcome, SchedulerError> {
        let (owned, job_ids): (BTreeSet<_>, BTreeSet<_>) = job_ids
            .into_iter()
            .partition(|job_id| self.state.owner_of(*job_id).is_some());
        let mut outcome = self.state.queue.cancel_jobs(job_ids, origin);
        for job_id in owned {
            let result = self.act(job_id, QueueAction::Cancel(origin)).await;
            outcome.insert(job_id, result);
        }
        Ok(outcome)
    }

    // Update a job's category and/or metadata.
    pub async fn update_job(&self, job_id: JobId, update: JobUpdate) -> Result<(), SchedulerError> {
        self.act(job_id, QueueAction::Update(update)).await
    }

    // Update a finished job's history row. The password field is ignored.
    pub async fn update_history(
        &self,
        job_id: JobId,
        update: JobUpdate,
    ) -> Result<(), SchedulerError> {
        self.state
            .queue
            .clone()
            .update_history(job_id, update)
            .await
    }

    // Move a job within the manual queue order. Durable and reflected in
    // queue listings; breaks ties within a dispatch priority band rather than
    // overriding HIGH/NORMAL/LOW.
    pub async fn reorder_job(
        &self,
        job_id: JobId,
        target: QueueMoveTarget,
    ) -> Result<(), SchedulerError> {
        self.act(job_id, QueueAction::Move(target)).await
    }

    // Move multiple jobs within the manual queue order in a single round
    // trip. All moves are applied atomically: if any job id is unknown, no
    // move is applied and `Err(SchedulerError::JobNotFound)` is returned.
    // The resulting order is persisted and the job snapshot is published
    // once for the whole batch, rather than once per move.
    pub async fn reorder_jobs(
        &self,
        moves: Vec<(JobId, QueueMoveTarget)>,
    ) -> Result<(), SchedulerError> {
        // The pipeline's moves go as one batch; another owner's go to it.
        let (owned, moves): (Vec<_>, Vec<_>) = moves
            .into_iter()
            .partition(|(job_id, _)| self.state.owner_of(*job_id).is_some());
        if !moves.is_empty() || owned.is_empty() {
            self.state.queue.reorder_jobs(moves)?;
        }
        for (job_id, target) in owned {
            self.act(job_id, QueueAction::Move(target)).await?;
        }
        Ok(())
    }

    // Publish the jobs another owner keeps beside the pipeline's. What the
    // publish changed goes out on the job events, as the pipeline's own
    // changes do: a job that appears is created, one that changes says so.
    // A job that leaves is retired, which says how it ended.
    pub fn publish_owned_jobs(&self, jobs: Vec<JobInfo>) {
        for event in self.state.publish_torrent_jobs(jobs) {
            let _ = self.event_tx.send(event);
        }
    }

    // A job another owner finished joins the finished jobs, once, and its
    // ending goes out with the history that holds it.
    pub async fn retire_job(&self, info: JobInfo) -> Result<(), SchedulerError> {
        let queue = &self.state.queue;
        queue.write(|inner| {
            let ended = info.ended_event();
            let job_id = info.job_id;
            inner.torrents.retain(|job| job.job_id != job_id);
            let mut history: Vec<_> = queue
                .history()
                .iter()
                .filter(|job| job.job_id != job_id)
                .cloned()
                .collect();
            history.insert(0, info);
            history.truncate(FINISHED_JOBS_RUNTIME_CAP);
            queue.replace_history(inner, history.into());
            if let Some(event) = ended {
                let _ = self.event_tx.send(event);
            }
            Change::changed(())
        });
        Ok(())
    }

    pub fn state(&self) -> &SharedPipelineState {
        &self.state
    }

    // Reprocess a completed or failed job (re-run post-download stages without re-downloading).
    pub async fn reprocess_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.reprocess_job_with_password(job_id, None).await
    }

    pub async fn reprocess_job_with_password(
        &self,
        job_id: JobId,
        password: Option<String>,
    ) -> Result<(), SchedulerError> {
        self.state
            .queue
            .clone()
            .restart(job_id, password, true, self.event_tx.clone())
            .await
    }

    // Re-download a completed or failed job from its persisted NZB under the same job ID.
    pub async fn redownload_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.redownload_job_with_password(job_id, None).await
    }

    pub async fn redownload_job_with_password(
        &self,
        job_id: JobId,
        password: Option<String>,
    ) -> Result<(), SchedulerError> {
        self.state
            .queue
            .clone()
            .restart(job_id, password, false, self.event_tx.clone())
            .await
    }

    // Delete a completed/failed/cancelled job from history.
    //
    // An `Err` does not say the record survived: a cleanup that failed after
    // the record was deleted is returned as its error too. A caller that must
    // know whether the record is gone reads `delete_history_batch`.
    pub async fn delete_history(
        &self,
        job_id: JobId,
        delete_files: bool,
    ) -> Result<HistoryDeleteOutcome, SchedulerError> {
        let mut outcome = self
            .delete_history_batch(BTreeSet::from([job_id]), delete_files)
            .await?;
        if let Some(error) = outcome.refused.remove(&job_id) {
            return Err(error);
        }
        outcome
            .deleted
            .remove(&job_id)
            .unwrap_or(Ok(Default::default()))
    }

    // Delete a batch of completed/failed/cancelled jobs from history in one
    // pipeline command. Callers holding more than `HISTORY_DELETE_BATCH_SIZE`
    // ids send them in batches of that size.
    pub async fn delete_history_batch(
        &self,
        job_ids: BTreeSet<JobId>,
        delete_files: bool,
    ) -> Result<HistoryDeleteBatchOutcome, SchedulerError> {
        self.state
            .queue
            .clone()
            .delete_history(job_ids, delete_files)
            .await
    }

    // Get info about a specific job (reads from shared state, no channel round-trip).
    pub fn get_job(&self, job_id: JobId) -> Result<JobInfo, SchedulerError> {
        self.state
            .get_job(job_id)
            .ok_or(SchedulerError::JobNotFound(job_id))
    }

    // List all jobs (reads from shared state, no channel round-trip).
    pub fn list_jobs(&self) -> Vec<JobInfo> {
        self.state.list_jobs()
    }

    // History excludes every row still owned by the queue, even after it is hidden.
    pub fn queued_job_ids(&self) -> HashSet<JobId> {
        self.state.queued_job_ids()
    }

    // How many jobs are in each status (reads from shared state, copies no
    // job).
    pub fn job_status_counts(&self) -> crate::operations::metrics_store::JobStatusCounts {
        self.state.job_status_counts()
    }

    // The revision of the published job list; it moves on every publish.
    pub fn job_revision(&self) -> u64 {
        self.state.job_revision()
    }

    // Wakes when the published job list changes.
    pub fn subscribe_job_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.state.subscribe_job_changes()
    }

    // Download rate of every transferring job (reads from shared state, no
    // channel round-trip). See [`SharedPipelineState::job_download_rates`].
    pub fn job_download_rates(&self) -> Vec<(JobId, u64)> {
        self.state.job_download_rates()
    }

    // The hardware profile in force and the scheduled one overriding the
    // operator's choice (reads from shared state, no channel round-trip).
    pub fn hardware_profile_in_force(&self) -> Option<HardwareProfileInForce> {
        self.state.hardware_profile_in_force()
    }

    // Get current metrics (reads from shared state, no channel round-trip).
    pub fn get_metrics(&self) -> MetricsSnapshot {
        self.state.metrics_snapshot()
    }

    // The live pipeline counters the download workers book into.
    pub fn pipeline_metrics(&self) -> Arc<PipelineMetrics> {
        Arc::clone(self.state.metrics())
    }

    // How long this process has spent downloading.
    pub fn download_time(&self) -> std::time::Duration {
        self.state.metrics().download_time()
    }

    pub fn get_extraction_rejections(&self) -> [u64; 9] {
        self.state.metrics().extraction_rejections()
    }

    // Whether the pipeline task is gone.
    //
    // The runtime guard closes the queue when the pipeline task exits.
    pub fn is_closed(&self) -> bool {
        self.state.queue.closed.load(Ordering::Acquire)
    }

    // Count one job accepted into the pipeline, labelled by where it came
    // from and which category it landed in.
    //
    // Called from the submission path, which is the only place that knows the
    // origin. Low-frequency by construction (one call per submitted job), so
    // the label map behind it is never contended.
    pub fn note_job_submitted(
        &self,
        origin: crate::jobs::SubmissionOrigin,
        category: Option<&str>,
    ) {
        self.state
            .metrics()
            .job_lifecycle
            .note_submitted(origin.as_str(), category.unwrap_or(""));
    }

    // Per-server article attempt counters and latency for the servers of the
    // currently active NNTP generation.
    //
    // Read on demand rather than folded into [`MetricsSnapshot`]: the 100 ms
    // snapshot tick must stay a fixed-size struct copy with no `Vec` in it.
    // This reads through a `RwLock` that is only ever write-locked when a new
    // NNTP runtime generation is activated, so it never contends with the
    // per-article path that increments the counters.
    pub fn server_metrics_snapshot(
        &self,
    ) -> Vec<crate::operations::instrumentation::ServerMetricsSnapshot> {
        self.state.metrics().server_metrics.snapshot()
    }

    // Job lifecycle counters and duration histograms. Read on demand; see
    // [`Self::server_metrics_snapshot`] for why this is not in the tick.
    pub fn job_lifecycle_metrics_snapshot(
        &self,
    ) -> crate::operations::instrumentation::JobLifecycleMetricsSnapshot {
        self.state.metrics().job_lifecycle.snapshot()
    }

    // Pipeline stage duration histograms. Read on demand; see
    // [`Self::server_metrics_snapshot`] for why this is not in the tick.
    pub fn pipeline_histograms_snapshot(
        &self,
    ) -> crate::operations::instrumentation::PipelineHistogramsSnapshot {
        self.state.metrics().pipeline_histograms.snapshot()
    }

    // Get a fresh atomics-based metrics snapshot without advancing the shared
    // speed tracker.
    pub fn get_live_metrics(&self) -> MetricsSnapshot {
        self.state.raw_metrics_snapshot()
    }

    pub fn get_download_block(&self) -> DownloadBlockState {
        self.state.download_block()
    }

    // Per-server BODY transport state as last published by the download lanes.
    pub fn download_transport_health(&self) -> Vec<ServerTransportHealth> {
        self.state.download_transport_health()
    }

    // Record a connection test's first-byte latency for the download lanes.
    pub fn note_server_probe_latency(&self, server_id: u32, latency: Duration) {
        self.state.note_server_probe_latency(server_id, latency);
    }

    // Read-only pipeline internals for the diagnostics package. The pipeline
    // builds the snapshot at the top of its next turn after the request is
    // recorded; only this endpoint pays for it.
    pub async fn pipeline_diagnostics(
        &self,
    ) -> Result<crate::pipeline::diagnostics::PipelineDiagnostics, SchedulerError> {
        let queue = &self.state.queue;
        let mut changes = queue.subscribe();
        let wanted = queue.write_intent(|inner| {
            inner.diagnostics_requested += 1;
            Change::unchanged(inner.diagnostics_requested)
        });
        loop {
            let answer = queue.read(|inner| {
                inner
                    .diagnostics
                    .as_ref()
                    .filter(|(answered, _)| *answered >= wanted)
                    .map(|(_, diagnostics)| diagnostics.clone())
            });
            if let Some(diagnostics) = answer {
                return Ok(diagnostics);
            }
            if queue.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(SchedulerError::ChannelClosed);
            }
            changes
                .changed()
                .await
                .map_err(|_| SchedulerError::ChannelClosed)?;
        }
    }

    // Put a hold in force. Every pause, whoever asks for it, comes through
    // here: the queue records it at once, and the pipeline puts it in force
    // at its next turn.
    pub async fn pause(&self, hold: Hold) -> Result<(), SchedulerError> {
        self.state.hold(hold);
        Ok(())
    }

    // Lift what `origin` holds over `owner`. Every resume comes through
    // here.
    pub async fn resume(&self, origin: HoldOrigin, owner: HoldOwner) -> Result<(), SchedulerError> {
        self.state.release(origin, owner);
        Ok(())
    }

    // The operator's pause of the whole queue.
    pub async fn pause_all(&self) -> Result<(), SchedulerError> {
        self.pause(Hold::manual(HoldOwner::All)).await
    }

    pub async fn resume_all(&self) -> Result<(), SchedulerError> {
        self.resume(HoldOrigin::Manual, HoldOwner::All).await
    }

    // The operator's pause of every torrent, as one hold.
    pub async fn pause_torrents(&self) -> Result<(), SchedulerError> {
        self.pause(Hold::manual(HoldOwner::Torrents)).await
    }

    pub async fn resume_torrents(&self) -> Result<(), SchedulerError> {
        self.resume(HoldOrigin::Manual, HoldOwner::Torrents).await
    }

    pub async fn pause_post_processing(&self) -> Result<(), SchedulerError> {
        self.state.set_post_processing_paused(true);
        Ok(())
    }

    pub async fn resume_post_processing(&self) -> Result<(), SchedulerError> {
        self.state.set_post_processing_paused(false);
        Ok(())
    }

    pub async fn cancel_post_processing(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            let Some(row) = inner
                .jobs
                .iter_mut()
                .find(|row| row.id == job_id && row.report.post_processing_cancellable)
            else {
                return Change::unchanged(Err(SchedulerError::Conflict(format!(
                    "job {} has no queued or active post-processing attempt",
                    job_id.0
                ))));
            };
            row.intents.cancel_post_processing = true;
            row.intents.seq += 1;
            Change::changed(Ok(()))
        })
    }

    // Check whether the pipeline is globally paused (reads from shared state).
    pub fn is_globally_paused(&self) -> bool {
        self.state.is_paused()
    }

    // Hold or release download admission for the schedule evaluator. The
    // reason is published with the download block so the hold is visible.
    pub(crate) fn set_schedule_admission_hold(&self, reason: Option<String>) {
        let held = reason.is_some();
        self.state.queue.write_intent(|inner| {
            inner.globals.schedule_hold_reason = reason;
            inner.globals.schedule_replay_paused = held;
            Change::changed(())
        });
    }

    pub fn is_post_processing_paused(&self) -> bool {
        self.state.is_post_processing_paused()
    }

    // Set the global bandwidth limits as the operator's edit. They are in
    // force at once, and a scheduled limit gives way until the next
    // scheduled speed rule fires.
    pub async fn set_bandwidth(
        &self,
        bandwidth: crate::proxies::Bandwidth,
    ) -> Result<(), SchedulerError> {
        self.send_bandwidth(bandwidth, true).await
    }

    // Save the global bandwidth limits as the operator's edit: the store,
    // then the shared configuration, then the limiter.
    pub async fn save_bandwidth(
        &self,
        db: &crate::Database,
        config: &crate::settings::SharedConfig,
        bandwidth: crate::proxies::Bandwidth,
    ) -> Result<(), SchedulerError> {
        let store = db.clone();
        tokio::task::spawn_blocking(move || store.set_network_bandwidth(&bandwidth))
            .await
            .map_err(|error| SchedulerError::Io(std::io::Error::other(error)))??;
        config.write().await.bandwidth = bandwidth;
        self.set_bandwidth(bandwidth).await
    }

    // Save the global download limit, keeping the upload limit as it is.
    pub async fn save_download_limit(
        &self,
        db: &crate::Database,
        config: &crate::settings::SharedConfig,
        download_bps: Option<u64>,
    ) -> Result<crate::proxies::Bandwidth, SchedulerError> {
        let bandwidth = crate::proxies::Bandwidth {
            download_bps,
            ..config.read().await.bandwidth
        };
        self.save_bandwidth(db, config, bandwidth).await?;
        Ok(bandwidth)
    }

    // Load the saved global bandwidth limits, as a configuration reload
    // does. A scheduled limit in force still applies under them.
    pub async fn restore_bandwidth(
        &self,
        bandwidth: crate::proxies::Bandwidth,
    ) -> Result<(), SchedulerError> {
        self.send_bandwidth(bandwidth, false).await
    }

    async fn send_bandwidth(
        &self,
        bandwidth: crate::proxies::Bandwidth,
        replaces_schedule: bool,
    ) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            let split_changed = inner.globals.bandwidth.split != bandwidth.split;
            inner.globals.bandwidth = bandwidth;
            // An operator's edit is in force at once: the schedule's rate
            // gives way until its next rule fires. A split the operator sets
            // gives way to no scheduled one until the next split rule fires;
            // editing the limits alone leaves a scheduled split in force.
            if replaces_schedule {
                inner.globals.scheduled_bandwidth = None;
                if split_changed {
                    inner.globals.scheduled_split = None;
                }
            }
            Change::changed(())
        });
        Ok(())
    }

    pub async fn set_max_concurrent_nzbs(
        &self,
        limit: std::num::NonZeroU32,
    ) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.max_concurrent_nzbs = Some(limit);
            crate::pipeline::queue::Change::changed(())
        });
        Ok(())
    }

    pub async fn set_propagation_delay(&self, seconds: u32) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.propagation_delay = Duration::from_secs(u64::from(seconds));
            Change::changed(())
        });
        Ok(())
    }

    // Apply a scheduled action (called by the schedule evaluator).
    pub async fn apply_schedule_action(
        &self,
        action: crate::bandwidth::ScheduleAction,
    ) -> Result<(), SchedulerError> {
        use crate::bandwidth::ScheduleAction;
        match &action {
            // Pause stops downloading and leaves what is complete seeding;
            // pause all stops seeding too.
            ScheduleAction::Pause | ScheduleAction::PauseAll | ScheduleAction::PauseTorrents => {
                let (scope, owner) = match action {
                    ScheduleAction::Pause => (HoldScope::Downloads, HoldOwner::All),
                    ScheduleAction::PauseAll => (HoldScope::Everything, HoldOwner::All),
                    _ => (HoldScope::Everything, HoldOwner::Torrents),
                };
                let hold = Hold {
                    origin: HoldOrigin::Scheduled,
                    scope,
                    owner,
                };
                self.state.hold(hold);
                tracing::info!(?hold, "schedule: paused");
                return Ok(());
            }
            ScheduleAction::Resume | ScheduleAction::ResumeTorrents => {
                let owner = match action {
                    ScheduleAction::Resume => HoldOwner::All,
                    _ => HoldOwner::Torrents,
                };
                self.state.release(HoldOrigin::Scheduled, owner);
                tracing::info!(?owner, "schedule: resumed");
                return Ok(());
            }
            ScheduleAction::PausePostProcessing => return self.pause_post_processing().await,
            ScheduleAction::ResumePostProcessing => return self.resume_post_processing().await,
            ScheduleAction::HardwareProfile { profile } => {
                return self.set_scheduled_hardware_profile(Some(*profile)).await;
            }
            ScheduleAction::SpeedLimit { limits } => {
                // Only the global limit lives here; an egress or provider
                // limit is that holder's own rate.
                match limits
                    .iter()
                    .rev()
                    .find(|limit| limit.target == crate::bandwidth::SpeedTarget::Global)
                {
                    Some(limit) => {
                        let scheduled = crate::proxies::Bandwidth {
                            download_bps: limit.bytes_per_sec,
                            upload_bps: limit.upload_bytes_per_sec,
                            split: None,
                        };
                        self.state.queue.write_intent(|inner| {
                            inner.globals.scheduled_bandwidth = Some(scheduled);
                            Change::changed(())
                        });
                        tracing::info!(?scheduled, "schedule: set speed limit");
                    }
                    None => tracing::warn!(
                        ?limits,
                        "an egress or provider speed limit reached download pipeline"
                    ),
                }
                return Ok(());
            }
            ScheduleAction::BandwidthSplit { usenet_pct } => {
                let split = match usenet_pct.map(crate::bandwidth::BandwidthSplit::new) {
                    None => None,
                    Some(Ok(split)) => Some(split),
                    Some(Err(error)) => {
                        tracing::warn!(?usenet_pct, %error, "schedule: bandwidth split refused");
                        return Ok(());
                    }
                };
                self.state.queue.write_intent(|inner| {
                    inner.globals.scheduled_split = split;
                    Change::changed(())
                });
                tracing::info!(?usenet_pct, "schedule: set bandwidth split");
                return Ok(());
            }
            ScheduleAction::SetQuotaMetering { enabled, target } => match target {
                crate::bandwidth::QuotaTarget::AllEgresses => {
                    if let Some(policy) = self.state.server_transfer_policy() {
                        policy.set_egress_quota_metering(*enabled);
                    }
                    self.state.queue.write_intent(|_| {
                        self.state
                            .queue
                            .counters
                            .bandwidth
                            .set_metering_enabled(*enabled);
                        Change::changed(())
                    });
                }
                crate::bandwidth::QuotaTarget::Egress(id) => {
                    if let Some(policy) = self.state.server_transfer_policy() {
                        policy.set_one_egress_quota_metering(*id, *enabled);
                    }
                }
            },
            _ => tracing::warn!(?action, "service schedule action reached download pipeline"),
        }
        Ok(())
    }

    // Record the operator's hardware profile for the next dispatch boundary;
    // work already running keeps the limits it started with.
    pub async fn set_hardware_profile(
        &self,
        profile: crate::runtime::HardwareProfile,
    ) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.hardware_profile = Some(profile);
            Change::changed(())
        });
        Ok(())
    }

    // Put the schedule's hardware profile in force, or with `None` return to
    // the operator's choice (called by the schedule evaluator).
    pub async fn set_scheduled_hardware_profile(
        &self,
        profile: Option<crate::runtime::HardwareProfile>,
    ) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.scheduled_hardware_profile = profile;
            Change::changed(())
        });
        Ok(())
    }

    pub fn set_server_transfer_policy(
        &self,
        registry: Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
    ) {
        self.state.set_server_transfer_policy(registry);
    }

    pub fn server_transfer_policy(
        &self,
    ) -> Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>> {
        self.state.server_transfer_policy()
    }

    // The speed test service while something holds it.
    pub fn speed_test_service(&self) -> Option<crate::speed_test::SpeedTestService> {
        self.state
            .speed_test
            .lock()
            .expect("speed test slot")
            .upgrade()
            .map(crate::speed_test::SpeedTestService::from_inner)
    }

    // The speed test service, created by `init` when none is held.
    pub fn speed_test_service_or_init(
        &self,
        init: impl FnOnce() -> crate::speed_test::SpeedTestService,
    ) -> crate::speed_test::SpeedTestService {
        let mut slot = self.state.speed_test.lock().expect("speed test slot");
        if let Some(inner) = slot.upgrade() {
            return crate::speed_test::SpeedTestService::from_inner(inner);
        }
        let service = init();
        *slot = service.downgrade();
        service
    }

    pub fn set_proxy_runtime(&self, runtime: Arc<crate::proxies::ProxyRuntime>) {
        *self.state.proxy_runtime.write().expect("proxy runtime") = Some(runtime);
    }

    pub fn proxy_runtime(&self) -> Option<Arc<crate::proxies::ProxyRuntime>> {
        self.state.proxy_runtime()
    }

    pub fn set_nntp_pool(&self, pool: Arc<weaver_nntp::pool::NntpPool>) {
        self.state.set_nntp_pool(pool);
    }

    pub fn nntp_pool(&self) -> Option<Arc<weaver_nntp::pool::NntpPool>> {
        self.state.nntp_pool()
    }

    pub fn nntp_runtime_activation(&self) -> Option<NntpRuntimeActivation> {
        self.state.nntp_runtime_activation()
    }

    // Replace the NNTP client at runtime (e.g. after server config changes).
    pub async fn rebuild_nntp<T: Send + 'static>(
        &self,
        client: T,
        total_connections: usize,
    ) -> Result<NntpRuntimeActivation, SchedulerError> {
        let client: Box<dyn std::any::Any + Send> = Box::new(client);
        let client = client
            .downcast::<weaver_nntp::client::NntpClient>()
            .map_err(|_| {
                SchedulerError::Internal(
                    "invalid NNTP runtime client supplied for activation".into(),
                )
            })?;
        // A rebuild the pipeline has not taken yet is superseded; its client
        // leaves the lock with the write and is dropped after it.
        let (activation, _superseded) = self.state.queue.write_intent(|inner| {
            inner.globals.nntp_generation += 1;
            let generation = inner.globals.nntp_generation;
            let superseded = inner.nntp.replace((generation, *client, total_connections));
            Change::changed((
                NntpRuntimeActivation {
                    generation,
                    configured_connections: total_connections,
                },
                superseded,
            ))
        });
        Ok(activation)
    }

    // Update disk tuning after the background startup measurement completes.
    pub async fn update_random_read_iops(
        &self,
        random_read_iops: f64,
    ) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.random_read_iops = Some(random_read_iops);
            Change::changed(())
        });
        Ok(())
    }

    // Update the pipeline's runtime directories for future jobs.
    pub async fn update_runtime_paths(
        &self,
        data_dir: PathBuf,
        intermediate_dir: PathBuf,
        complete_dir: PathBuf,
    ) -> Result<(), SchedulerError> {
        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::create_dir_all(&intermediate_dir).await?;
        tokio::fs::create_dir_all(&complete_dir).await?;
        self.state.queue.write_intent(|inner| {
            inner.globals.runtime_paths = Some(crate::pipeline::queue::RuntimePaths {
                data: data_dir,
                intermediate: intermediate_dir,
                complete: complete_dir,
            });
            Change::changed(())
        });
        Ok(())
    }

    // Subscribe to job-level pipeline events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<PipelineEvent> {
        self.event_tx.subscribe()
    }

    // Subscribe to the per-article event stream, which the job-level
    // broadcast does not carry.
    pub fn subscribe_segment_events(&self) -> broadcast::Receiver<PipelineEvent> {
        self.state.subscribe_segment_events()
    }

    // Signal shutdown.
    pub async fn shutdown(&self) -> Result<(), SchedulerError> {
        self.state.queue.write_intent(|inner| {
            inner.globals.shutting_down = true;
            Change::changed(())
        });
        // An e2e hook held until shutdown is released by the same signal
        // the pipeline reads.
        crate::e2e_failpoint::release_shutdown_holds();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
