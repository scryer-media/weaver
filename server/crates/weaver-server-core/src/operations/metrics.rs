// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::operations::instrumentation::{
    JobLifecycleMetrics, PipelineHistograms, ServerMetricsRegistry,
};

mod par3;

pub use par3::{
    PAR3_MEMORY_CATEGORIES, PAR3_SLOTS, PAR3_STALL_THRESHOLD_MS, Par3AdmissionReason,
    Par3EngineNarrowing, Par3EngineRefusal, Par3Metrics, Par3MetricsSnapshot, Par3OutcomeClass,
    Par3Phase, Par3Slot, Par3SlotSnapshot, Par3Stage, par3_memory_category_names,
};

const SPEED_WINDOW_SAMPLES: usize = 50; // ~5 seconds at 100ms snapshot rate
const SPEED_EMA_HALF_LIFE_SECS: f64 = 1.0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadPressureState {
    #[default]
    Clear,
    Soft,
    Hard,
}

impl DownloadPressureState {
    pub const fn as_code(self) -> usize {
        match self {
            Self::Clear => 0,
            Self::Soft => 1,
            Self::Hard => 2,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::Soft => "soft",
            Self::Hard => "hard",
        }
    }

    pub const fn from_code(code: usize) -> Self {
        match code {
            1 => Self::Soft,
            2 => Self::Hard,
            _ => Self::Clear,
        }
    }
}

// A clause of the article scheduler's "may this server fetch this article"
// answer, as the per-clause hot-job block counters name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerBlockClause {
    // The job is too young to fetch from at all.
    Propagation,
    // The job's posts are older than the server's retention.
    Retention,
    // The article excludes the server, or its rotation hint points away.
    ServerExclusion,
    // A backfill server waits for the fill servers to give the article up.
    Backfill,
    // The recovery metadata bootstrap claims the queue for its index files.
    RecoveryMetadataBootstrap,
    // The UU spool cursor has not reached the article.
    UuCursor,
    // The restart checkpoint holds the job's undurable lead.
    Checkpoint,
    // A higher-priority group can still supply this article.
    ProviderPriority,
}

impl SchedulerBlockClause {
    pub const COUNT: usize = 8;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Propagation,
        Self::Retention,
        Self::ServerExclusion,
        Self::Backfill,
        Self::RecoveryMetadataBootstrap,
        Self::UuCursor,
        Self::Checkpoint,
        Self::ProviderPriority,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Propagation => "propagation",
            Self::Retention => "retention",
            Self::ServerExclusion => "server_exclusion",
            Self::Backfill => "backfill",
            Self::RecoveryMetadataBootstrap => "recovery_metadata_bootstrap",
            Self::UuCursor => "uu_cursor",
            Self::Checkpoint => "checkpoint",
            Self::ProviderPriority => "provider_priority",
        }
    }

    // The clause's slot in the per-clause counters.
    pub fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadPressureReason {
    #[default]
    None,
    Decode,
    Write,
    DecodeAndWrite,
}

impl DownloadPressureReason {
    pub const fn as_code(self) -> usize {
        match self {
            Self::None => 0,
            Self::Decode => 1,
            Self::Write => 2,
            Self::DecodeAndWrite => 3,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Decode => "decode",
            Self::Write => "write",
            Self::DecodeAndWrite => "decode_and_write",
        }
    }

    pub const fn from_code(code: usize) -> Self {
        match code {
            1 => Self::Decode,
            2 => Self::Write,
            3 => Self::DecodeAndWrite,
            _ => Self::None,
        }
    }
}

// Exhaustive variant lists for the label sets these enums drive. The
// Prometheus exporter renders one series per variant, so a variant added
// without a matching label silently disappears from `/metrics`; deriving the
// label set from `ALL` makes that impossible.

impl DownloadPressureState {
    pub const ALL: [Self; 3] = [Self::Clear, Self::Soft, Self::Hard];
}

impl DownloadPressureReason {
    pub const ALL: [Self; 4] = [Self::None, Self::Decode, Self::Write, Self::DecodeAndWrite];
}

// Short-window rate for one monotonically increasing counter.
//
// Holds a ring buffer of `(timestamp, cumulative)` samples and smooths the
// window's raw rate with a 1 s half-life EMA, so the published value follows
// pipeline ticks without showing every short-lived burst. Not hot-path code:
// it is advanced once per 100 ms metrics tick under the tracker mutex, and
// once per active job phase on that same tick, so a job's displayed rate and
// the global gauge are one estimator over one window and can be compared.
#[derive(Debug, Clone)]
pub(crate) struct RateSeries {
    // Ring buffer of (timestamp, cumulative value) samples.
    samples: Vec<(Instant, u64)>,
    // Next write position in the ring buffer.
    pos: usize,
    // Last computed EMA-smoothed rate (units/sec).
    rate: f64,
    // Timestamp of the last EMA update.
    last_ema_at: Option<Instant>,
}

impl RateSeries {
    pub(crate) fn new() -> Self {
        Self {
            samples: Vec::with_capacity(SPEED_WINDOW_SAMPLES),
            pos: 0,
            rate: 0.0,
            last_ema_at: None,
        }
    }

    // A series on this one's sample instants that has counted nothing yet:
    // every past sample holds `cumulative` and the smoothed rate is zero.
    //
    // A counter that starts contributing to a total mid-window gets one of
    // these from the total's series. Both are then the same linear estimator
    // over the same instants, so the parts' rates sum to the total's rate.
    pub(crate) fn aligned_from(&self, cumulative: u64) -> Self {
        Self {
            samples: self
                .samples
                .iter()
                .map(|&(at, _)| (at, cumulative))
                .collect(),
            pos: self.pos,
            rate: 0.0,
            last_ema_at: self.last_ema_at,
        }
    }

    // Record a sample and recompute the smoothed rate.
    pub(crate) fn update(&mut self, now: Instant, cumulative: u64) -> f64 {
        if self.samples.len() < SPEED_WINDOW_SAMPLES {
            self.samples.push((now, cumulative));
        } else {
            self.samples[self.pos] = (now, cumulative);
        }
        self.pos = (self.pos + 1) % SPEED_WINDOW_SAMPLES;

        // Compare newest sample to oldest in the current window, then smooth
        // the raw rate with a 1s half-life EMA.
        let oldest_idx = if self.samples.len() < SPEED_WINDOW_SAMPLES {
            0
        } else {
            self.pos % self.samples.len()
        };
        let oldest = self.samples[oldest_idx];

        let dt = now.duration_since(oldest.0).as_secs_f64();
        if dt > 0.05 {
            let delta = cumulative.saturating_sub(oldest.1);
            let raw_rate = delta as f64 / dt;

            if let Some(last_ema_at) = self.last_ema_at {
                let elapsed = now.duration_since(last_ema_at).as_secs_f64();
                if elapsed > 0.0 {
                    let alpha = 1.0 - 0.5_f64.powf(elapsed / SPEED_EMA_HALF_LIFE_SECS);
                    self.rate += alpha * (raw_rate - self.rate);
                } else {
                    self.rate = raw_rate;
                }
            } else {
                self.rate = raw_rate;
            }

            self.last_ema_at = Some(now);
            if self.rate < f64::EPSILON {
                self.rate = 0.0;
            }
        }
        self.rate
    }
}

// Short-window rates published alongside the metrics snapshot.
#[derive(Debug, Clone, Copy, Default)]
struct CurrentRates {
    // Downloaded bytes per second.
    download_bytes: u64,
    // Articles (segments) downloaded per second.
    articles: f64,
    // Decoded MiB per second.
    decode_mib: f64,
}

// Tracks the three published "current" rates over a sliding window.
//
// All three were once lifetime averages since process start, which made them
// useless as live indicators — a box that downloaded fast for an hour and then
// stalled kept reporting a healthy rate. They are now short-window rates
// computed on the same 100 ms tick that already takes this mutex.
struct SpeedTracker {
    bytes_downloaded: RateSeries,
    segments_downloaded: RateSeries,
    bytes_decoded: RateSeries,
    last: CurrentRates,
    // Time spent downloading: the sum of the tick intervals in which bytes
    // arrived. Idle and paused stretches add nothing.
    download_time: Duration,
    // The previous tick and the downloaded total it saw.
    previous: Option<(Instant, u64)>,
}

impl SpeedTracker {
    fn new() -> Self {
        Self {
            bytes_downloaded: RateSeries::new(),
            segments_downloaded: RateSeries::new(),
            bytes_decoded: RateSeries::new(),
            last: CurrentRates::default(),
            download_time: Duration::ZERO,
            previous: None,
        }
    }

    // Record a sample of each series and recompute the published rates.
    fn update(
        &mut self,
        now: Instant,
        bytes_downloaded: u64,
        segments_downloaded: u64,
        bytes_decoded: u64,
    ) -> CurrentRates {
        if let Some((at, downloaded)) = self.previous
            && bytes_downloaded > downloaded
        {
            self.download_time += now.saturating_duration_since(at);
        }
        self.previous = Some((now, bytes_downloaded));
        // The byte rate keeps its historical rounding: sub-1 B/s reads as idle.
        let download_bytes = self.bytes_downloaded.update(now, bytes_downloaded);
        let download_bytes = if download_bytes < 1.0 {
            0
        } else {
            download_bytes as u64
        };
        let rates = CurrentRates {
            download_bytes,
            articles: self.segments_downloaded.update(now, segments_downloaded),
            decode_mib: self.bytes_decoded.update(now, bytes_decoded) / (1024.0 * 1024.0),
        };
        self.last = rates;
        rates
    }

    // The most recently computed rates, without advancing the window.
    fn last(&self) -> CurrentRates {
        self.last
    }
}

impl std::fmt::Debug for SpeedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeedTracker")
            .field("rates", &self.last)
            .finish()
    }
}

// Live metrics for the pipeline, updated atomically by various stages.
#[derive(Debug)]
pub struct PipelineMetrics {
    // Throughput
    pub bytes_downloaded: AtomicU64,
    pub bytes_decoded: AtomicU64,
    pub bytes_committed: AtomicU64,

    // Queue depths
    pub download_queue_depth: AtomicUsize,
    pub active_downloads: AtomicUsize,
    // Woken when `active_downloads` is published at zero after being above it.
    pub downloads_drained: tokio::sync::Notify,
    pub active_decodes: AtomicUsize,
    // Raw article bodies waiting for decode scheduling.
    pub decode_pending: AtomicUsize,
    pub decode_pending_bytes: AtomicU64,
    pub decode_active_bytes: AtomicU64,
    pub commit_pending: AtomicUsize,
    // All decoded bytes awaiting placement on disk, including UU spool files.
    pub write_pending_bytes: AtomicU64,
    // Resident decoded bytes awaiting placement on disk.
    pub write_buffered_bytes: AtomicU64,
    pub write_buffered_segments: AtomicUsize,
    // Disk-backed uuencode parts awaiting their missing prefix.
    pub uu_spooled_bytes: AtomicU64,
    pub uu_spooled_segments: AtomicUsize,
    pub direct_write_evictions: AtomicU64,
    // Delivered files renamed out of an obfuscated in-archive member name.
    pub deobfuscated_members_renamed: AtomicU64,
    pub decode_pressure_soft_limit_bytes: AtomicU64,
    pub decode_pressure_hard_limit_bytes: AtomicU64,
    pub write_pressure_soft_limit_bytes: AtomicU64,
    pub write_pressure_hard_limit_bytes: AtomicU64,
    pub download_pressure_state: AtomicUsize,
    pub download_pressure_reason: AtomicUsize,
    pub download_pressure_stalls_total: AtomicU64,
    pub download_pressure_stall_duration_ms: AtomicU64,
    pub download_pressure_current_stall_ms: AtomicU64,
    pub download_restart_durable_lead_blocked_total: AtomicU64,
    // Article-scheduler guard: the scheduler answered "nothing to hand out"
    // for a server while an eligible job still held an article that server
    // was allowed to fetch. The scheduler's own rule forbids that, so any
    // nonzero reading is a defect, not a tuning signal.
    pub download_scheduler_idle_with_servable_total: AtomicU64,
    // Handouts the article scheduler made from the job it considers hot.
    pub download_scheduler_handouts_total_hot: AtomicU64,
    // Handouts the article scheduler made from a job behind the hot one,
    // because the hot job could not serve the asking server.
    pub download_scheduler_handouts_total_spill: AtomicU64,
    // Handouts made for a probe rather than for payload throughput.
    pub download_scheduler_handouts_total_probe: AtomicU64,
    // Queued articles a scheduler queue scan looked at and passed over.
    pub download_scheduler_scan_items_skipped_total: AtomicU64,
    // Scheduler queue scans that found no article the server may fetch.
    pub download_scheduler_scan_no_match_total: AtomicU64,
    // Times the hot job had nothing for the asking server, counted once
    // for each clause that refused it, indexed by
    // [`SchedulerBlockClause::index`].
    pub download_scheduler_hot_blocked_total: [AtomicU64; SchedulerBlockClause::COUNT],
    // Raw article bytes dispatched to lanes and not yet answered, summed
    // across every lane. Sampled on the metrics tick from the dispatch
    // reservations, never maintained on an article path.
    pub download_lane_inflight_bytes: AtomicU64,
    // Jobs competing for articles right now: the hot job plus every job
    // behind it that still has something this pipeline could fetch.
    pub download_jobs_eligible: AtomicUsize,
    // One while a hot job exists, zero while nothing is being downloaded.
    pub download_jobs_hot: AtomicUsize,
    pub download_lanes_active: AtomicUsize,
    pub download_lanes_sequential_active: AtomicUsize,
    pub download_lanes_depth2_active: AtomicUsize,
    pub download_lanes_depth4_active: AtomicUsize,
    pub download_lanes_depth8_active: AtomicUsize,
    pub download_lanes_idle_active: AtomicUsize,
    pub download_lanes_awaiting_work_active: AtomicUsize,
    pub download_lanes_binding_server_active: AtomicUsize,
    pub download_lanes_acquired_active: AtomicUsize,
    pub download_lanes_issuing_active: AtomicUsize,
    pub download_lanes_draining_active: AtomicUsize,
    pub download_lanes_yield_after_batch_active: AtomicUsize,
    pub download_lanes_parking_active: AtomicUsize,
    pub download_lanes_recovering_active: AtomicUsize,
    pub download_lane_parks_no_work_total: AtomicU64,
    pub download_lane_parks_pressure_total: AtomicU64,
    pub download_lane_parks_probe_yield_total: AtomicU64,
    pub download_lane_parks_error_total: AtomicU64,
    pub download_lane_lease_items_total: AtomicU64,
    pub download_lane_refill_granted_total: AtomicU64,
    pub download_lane_refill_parked_total: AtomicU64,
    pub download_lane_refill_deferred_total: AtomicU64,
    pub download_lane_refill_saturated_total: AtomicU64,
    pub download_pipeline_trial_success_total: AtomicU64,
    pub download_pipeline_trial_failure_total: AtomicU64,
    pub download_pipeline_proof_pass_total: AtomicU64,
    pub download_pipeline_cooldown_total: AtomicU64,
    pub download_pipeline_replay_items_total: AtomicU64,

    // Counts
    pub segments_downloaded: AtomicU64,
    pub segments_decoded: AtomicU64,
    pub segments_committed: AtomicU64,
    pub articles_not_found: AtomicU64,
    // Articles booked missing because a 430 taught the retry nothing: the
    // exclusion set came back the same size it went out. Every increment is
    // a retry loop that would otherwise have re-asked the same servers at
    // zero delay forever, so a non-zero value here points at whatever lost
    // the article's exclusions on the way back.
    pub articles_not_found_without_new_server: AtomicU64,
    pub decode_errors: AtomicU64,

    // Post-processing activity
    pub verify_active: AtomicUsize,
    pub repair_active: AtomicUsize,
    pub extract_active: AtomicUsize,
    extraction_rejections: [AtomicU64; 9],
    pub disk_write_latency_us: AtomicU64,

    // Retry tracking
    pub segments_retried: AtomicU64,
    pub segments_failed_permanent: AtomicU64,
    pub parked_infrastructure_work: AtomicUsize,
    pub nntp_generation_recovery_requeues: AtomicU64,
    // Download dispatch stall recoveries, by escalation: a kick clears the
    // dispatch-side holds of a stall nothing explains; a release books the
    // stalled jobs' queued articles missing when the kick moved no bytes.
    pub download_dispatch_stall_kicks: AtomicU64,
    pub download_dispatch_stall_releases: AtomicU64,
    pub download_failures_article_not_found: AtomicU64,
    pub download_failures_capacity_unavailable: AtomicU64,
    pub download_failures_transient: AtomicU64,
    pub download_failures_auth: AtomicU64,
    pub download_failures_permanent: AtomicU64,

    // Speed — computed from bytes_downloaded delta, then smoothed via EMA
    speed_tracker: Mutex<SpeedTracker>,

    // Decode quality
    pub crc_errors: AtomicU64,

    // Recovery
    pub recovery_queue_depth: AtomicUsize,

    // Per-server article attempt counters and latency. Deliberately *not*
    // folded into [`MetricsSnapshot`]: the 100 ms tick must stay a fixed-size
    // struct copy, so this is read on demand through
    // [`crate::SchedulerHandle::server_metrics_snapshot`].
    pub server_metrics: ServerMetricsRegistry,
    // Job lifecycle counters and duration histograms, read on demand through
    // [`crate::SchedulerHandle::job_lifecycle_metrics_snapshot`].
    pub job_lifecycle: JobLifecycleMetrics,
    // Pipeline stage duration histograms, read on demand through
    // [`crate::SchedulerHandle::pipeline_histograms_snapshot`].
    pub pipeline_histograms: PipelineHistograms,
    // PAR3 recovery counters, phases and stalls. Unlike the three registries
    // above these *are* folded into [`MetricsSnapshot`]: every field is a
    // relaxed atomic over fixed storage, so the tick stays a struct copy.
    pub par3: Par3Metrics,

    // Timing (not atomic — set once at creation)
    pub start_time: Instant,
}

impl PipelineMetrics {
    // A job's post-processing lane moved: the gauges of the jobs verifying,
    // repairing and extracting follow it, each counting the jobs in its lane.
    pub fn post_state_moved(
        &self,
        from: crate::jobs::model::PostState,
        to: crate::jobs::model::PostState,
    ) {
        if from == to {
            return;
        }
        self.post_state_left(from);
        if let Some(gauge) = self.post_state_gauge(to) {
            gauge.fetch_add(1, Ordering::Relaxed);
        }
    }

    // A job left its post-processing lane for none: it was cancelled or
    // removed. A gauge never counts below zero, so a lane left that was
    // never counted cannot wrap it.
    pub fn post_state_left(&self, from: crate::jobs::model::PostState) {
        if let Some(gauge) = self.post_state_gauge(from) {
            let _ = gauge.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            });
        }
    }

    fn post_state_gauge(&self, state: crate::jobs::model::PostState) -> Option<&AtomicUsize> {
        use crate::jobs::model::PostState;
        match state {
            PostState::Verifying => Some(&self.verify_active),
            PostState::Repairing => Some(&self.repair_active),
            PostState::Extracting => Some(&self.extract_active),
            _ => None,
        }
    }

    // Publish the in-flight article count; waiters on a drain wake at zero.
    pub fn set_active_downloads(&self, active: usize) {
        let previous = self.active_downloads.swap(active, Ordering::AcqRel);
        if active == 0 && previous != 0 {
            self.downloads_drained.notify_waiters();
        }
    }

    // Wait until no article is in flight. The gauge is the condition.
    pub async fn wait_downloads_drained(&self) {
        loop {
            let drained = self.downloads_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.active_downloads.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes_downloaded: AtomicU64::new(0),
            bytes_decoded: AtomicU64::new(0),
            bytes_committed: AtomicU64::new(0),
            download_queue_depth: AtomicUsize::new(0),
            active_downloads: AtomicUsize::new(0),
            downloads_drained: tokio::sync::Notify::new(),
            active_decodes: AtomicUsize::new(0),
            decode_pending: AtomicUsize::new(0),
            decode_pending_bytes: AtomicU64::new(0),
            decode_active_bytes: AtomicU64::new(0),
            commit_pending: AtomicUsize::new(0),
            write_pending_bytes: AtomicU64::new(0),
            write_buffered_bytes: AtomicU64::new(0),
            write_buffered_segments: AtomicUsize::new(0),
            uu_spooled_bytes: AtomicU64::new(0),
            uu_spooled_segments: AtomicUsize::new(0),
            direct_write_evictions: AtomicU64::new(0),
            deobfuscated_members_renamed: AtomicU64::new(0),
            decode_pressure_soft_limit_bytes: AtomicU64::new(0),
            decode_pressure_hard_limit_bytes: AtomicU64::new(0),
            write_pressure_soft_limit_bytes: AtomicU64::new(0),
            write_pressure_hard_limit_bytes: AtomicU64::new(0),
            download_pressure_state: AtomicUsize::new(DownloadPressureState::Clear.as_code()),
            download_pressure_reason: AtomicUsize::new(DownloadPressureReason::None.as_code()),
            download_pressure_stalls_total: AtomicU64::new(0),
            download_pressure_stall_duration_ms: AtomicU64::new(0),
            download_pressure_current_stall_ms: AtomicU64::new(0),
            download_restart_durable_lead_blocked_total: AtomicU64::new(0),
            download_scheduler_idle_with_servable_total: AtomicU64::new(0),
            download_scheduler_handouts_total_hot: AtomicU64::new(0),
            download_scheduler_handouts_total_spill: AtomicU64::new(0),
            download_scheduler_handouts_total_probe: AtomicU64::new(0),
            download_scheduler_scan_items_skipped_total: AtomicU64::new(0),
            download_scheduler_scan_no_match_total: AtomicU64::new(0),
            download_scheduler_hot_blocked_total: std::array::from_fn(|_| AtomicU64::new(0)),
            download_lane_inflight_bytes: AtomicU64::new(0),
            download_jobs_eligible: AtomicUsize::new(0),
            download_jobs_hot: AtomicUsize::new(0),
            download_lanes_active: AtomicUsize::new(0),
            download_lanes_sequential_active: AtomicUsize::new(0),
            download_lanes_depth2_active: AtomicUsize::new(0),
            download_lanes_depth4_active: AtomicUsize::new(0),
            download_lanes_depth8_active: AtomicUsize::new(0),
            download_lanes_idle_active: AtomicUsize::new(0),
            download_lanes_awaiting_work_active: AtomicUsize::new(0),
            download_lanes_binding_server_active: AtomicUsize::new(0),
            download_lanes_acquired_active: AtomicUsize::new(0),
            download_lanes_issuing_active: AtomicUsize::new(0),
            download_lanes_draining_active: AtomicUsize::new(0),
            download_lanes_yield_after_batch_active: AtomicUsize::new(0),
            download_lanes_parking_active: AtomicUsize::new(0),
            download_lanes_recovering_active: AtomicUsize::new(0),
            download_lane_parks_no_work_total: AtomicU64::new(0),
            download_lane_parks_pressure_total: AtomicU64::new(0),
            download_lane_parks_probe_yield_total: AtomicU64::new(0),
            download_lane_parks_error_total: AtomicU64::new(0),
            download_lane_lease_items_total: AtomicU64::new(0),
            download_lane_refill_granted_total: AtomicU64::new(0),
            download_lane_refill_parked_total: AtomicU64::new(0),
            download_lane_refill_deferred_total: AtomicU64::new(0),
            download_lane_refill_saturated_total: AtomicU64::new(0),
            download_pipeline_trial_success_total: AtomicU64::new(0),
            download_pipeline_trial_failure_total: AtomicU64::new(0),
            download_pipeline_proof_pass_total: AtomicU64::new(0),
            download_pipeline_cooldown_total: AtomicU64::new(0),
            download_pipeline_replay_items_total: AtomicU64::new(0),
            segments_downloaded: AtomicU64::new(0),
            segments_decoded: AtomicU64::new(0),
            segments_committed: AtomicU64::new(0),
            articles_not_found: AtomicU64::new(0),
            articles_not_found_without_new_server: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            verify_active: AtomicUsize::new(0),
            repair_active: AtomicUsize::new(0),
            extract_active: AtomicUsize::new(0),
            extraction_rejections: std::array::from_fn(|_| AtomicU64::new(0)),
            disk_write_latency_us: AtomicU64::new(0),
            segments_retried: AtomicU64::new(0),
            segments_failed_permanent: AtomicU64::new(0),
            parked_infrastructure_work: AtomicUsize::new(0),
            nntp_generation_recovery_requeues: AtomicU64::new(0),
            download_dispatch_stall_kicks: AtomicU64::new(0),
            download_dispatch_stall_releases: AtomicU64::new(0),
            download_failures_article_not_found: AtomicU64::new(0),
            download_failures_capacity_unavailable: AtomicU64::new(0),
            download_failures_transient: AtomicU64::new(0),
            download_failures_auth: AtomicU64::new(0),
            download_failures_permanent: AtomicU64::new(0),
            speed_tracker: Mutex::new(SpeedTracker::new()),
            crc_errors: AtomicU64::new(0),
            recovery_queue_depth: AtomicUsize::new(0),
            server_metrics: ServerMetricsRegistry::new(),
            job_lifecycle: JobLifecycleMetrics::new(),
            pipeline_histograms: PipelineHistograms::new(),
            par3: Par3Metrics::default(),
            start_time: Instant::now(),
        })
    }

    // Monotonic milliseconds since this metrics object was created. Every
    // PAR3 timestamp shares this origin so a snapshot can subtract two of
    // them without needing a clock of its own.
    pub fn now_ms(&self) -> u64 {
        self.start_time
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    fn saturating_sub_u64(counter: &AtomicU64, amount: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(amount))
        });
    }

    pub(crate) fn note_extraction_rejection(&self, reason: &str) {
        const REASONS: [&str; 9] = [
            "unsafe_path",
            "unsupported_entry",
            "member_bytes",
            "job_bytes",
            "ratio",
            "entries",
            "deadline",
            "memory",
            "disk_reserve",
        ];
        if let Some(index) = REASONS.iter().position(|candidate| *candidate == reason) {
            self.extraction_rejections[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn extraction_rejections(&self) -> [u64; 9] {
        std::array::from_fn(|index| self.extraction_rejections[index].load(Ordering::Relaxed))
    }

    pub fn note_decode_work_queued(&self, raw_bytes: u64) {
        self.decode_pending.fetch_add(1, Ordering::Relaxed);
        self.decode_pending_bytes
            .fetch_add(raw_bytes, Ordering::Relaxed);
    }

    pub fn note_decode_work_released(&self, raw_bytes: u64) {
        let _ = self
            .decode_pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
        Self::saturating_sub_u64(&self.decode_pending_bytes, raw_bytes);
    }

    pub fn note_decode_task_started(&self, raw_bytes: u64) {
        let _ = self
            .decode_pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
        Self::saturating_sub_u64(&self.decode_pending_bytes, raw_bytes);
        self.decode_active_bytes
            .fetch_add(raw_bytes, Ordering::Relaxed);
    }

    pub fn note_decode_task_finished(&self, raw_bytes: u64) {
        Self::saturating_sub_u64(&self.decode_active_bytes, raw_bytes);
    }

    fn snapshot_with_speed(&self, bytes_downloaded: u64, rates: CurrentRates) -> MetricsSnapshot {
        let segments_downloaded = self.segments_downloaded.load(Ordering::Relaxed);
        let bytes_decoded = self.bytes_decoded.load(Ordering::Relaxed);
        let current_download_speed = rates.download_bytes;

        MetricsSnapshot {
            bytes_downloaded,
            bytes_decoded,
            bytes_committed: self.bytes_committed.load(Ordering::Relaxed),
            download_queue_depth: self.download_queue_depth.load(Ordering::Relaxed),
            active_downloads: self.active_downloads.load(Ordering::Relaxed),
            active_decodes: self.active_decodes.load(Ordering::Relaxed),
            decode_pending: self.decode_pending.load(Ordering::Relaxed),
            decode_pending_bytes: self.decode_pending_bytes.load(Ordering::Relaxed),
            decode_active_bytes: self.decode_active_bytes.load(Ordering::Relaxed),
            commit_pending: self.commit_pending.load(Ordering::Relaxed),
            write_pending_bytes: self.write_pending_bytes.load(Ordering::Relaxed),
            write_buffered_bytes: self.write_buffered_bytes.load(Ordering::Relaxed),
            write_buffered_segments: self.write_buffered_segments.load(Ordering::Relaxed),
            uu_spooled_bytes: self.uu_spooled_bytes.load(Ordering::Relaxed),
            uu_spooled_segments: self.uu_spooled_segments.load(Ordering::Relaxed),
            direct_write_evictions: self.direct_write_evictions.load(Ordering::Relaxed),
            deobfuscated_members_renamed: self.deobfuscated_members_renamed.load(Ordering::Relaxed),
            decode_pressure_soft_limit_bytes: self
                .decode_pressure_soft_limit_bytes
                .load(Ordering::Relaxed),
            decode_pressure_hard_limit_bytes: self
                .decode_pressure_hard_limit_bytes
                .load(Ordering::Relaxed),
            write_pressure_soft_limit_bytes: self
                .write_pressure_soft_limit_bytes
                .load(Ordering::Relaxed),
            write_pressure_hard_limit_bytes: self
                .write_pressure_hard_limit_bytes
                .load(Ordering::Relaxed),
            download_pressure_state: DownloadPressureState::from_code(
                self.download_pressure_state.load(Ordering::Relaxed),
            ),
            download_pressure_reason: DownloadPressureReason::from_code(
                self.download_pressure_reason.load(Ordering::Relaxed),
            ),
            download_pressure_stalls_total: self
                .download_pressure_stalls_total
                .load(Ordering::Relaxed),
            download_pressure_stall_duration_ms: self
                .download_pressure_stall_duration_ms
                .load(Ordering::Relaxed),
            download_pressure_current_stall_ms: self
                .download_pressure_current_stall_ms
                .load(Ordering::Relaxed),
            download_restart_durable_lead_blocked_total: self
                .download_restart_durable_lead_blocked_total
                .load(Ordering::Relaxed),
            download_scheduler_idle_with_servable_total: self
                .download_scheduler_idle_with_servable_total
                .load(Ordering::Relaxed),
            download_scheduler_handouts_total_hot: self
                .download_scheduler_handouts_total_hot
                .load(Ordering::Relaxed),
            download_scheduler_handouts_total_spill: self
                .download_scheduler_handouts_total_spill
                .load(Ordering::Relaxed),
            download_scheduler_handouts_total_probe: self
                .download_scheduler_handouts_total_probe
                .load(Ordering::Relaxed),
            download_scheduler_scan_items_skipped_total: self
                .download_scheduler_scan_items_skipped_total
                .load(Ordering::Relaxed),
            download_scheduler_scan_no_match_total: self
                .download_scheduler_scan_no_match_total
                .load(Ordering::Relaxed),
            download_scheduler_hot_blocked_total: std::array::from_fn(|index| {
                self.download_scheduler_hot_blocked_total[index].load(Ordering::Relaxed)
            }),
            download_lane_inflight_bytes: self.download_lane_inflight_bytes.load(Ordering::Relaxed),
            download_jobs_eligible: self.download_jobs_eligible.load(Ordering::Relaxed),
            download_jobs_hot: self.download_jobs_hot.load(Ordering::Relaxed),
            download_lanes_active: self.download_lanes_active.load(Ordering::Relaxed),
            download_lanes_sequential_active: self
                .download_lanes_sequential_active
                .load(Ordering::Relaxed),
            download_lanes_depth2_active: self.download_lanes_depth2_active.load(Ordering::Relaxed),
            download_lanes_depth4_active: self.download_lanes_depth4_active.load(Ordering::Relaxed),
            download_lanes_depth8_active: self.download_lanes_depth8_active.load(Ordering::Relaxed),
            download_lanes_idle_active: self.download_lanes_idle_active.load(Ordering::Relaxed),
            download_lanes_awaiting_work_active: self
                .download_lanes_awaiting_work_active
                .load(Ordering::Relaxed),
            download_lanes_binding_server_active: self
                .download_lanes_binding_server_active
                .load(Ordering::Relaxed),
            download_lanes_acquired_active: self
                .download_lanes_acquired_active
                .load(Ordering::Relaxed),
            download_lanes_issuing_active: self
                .download_lanes_issuing_active
                .load(Ordering::Relaxed),
            download_lanes_draining_active: self
                .download_lanes_draining_active
                .load(Ordering::Relaxed),
            download_lanes_yield_after_batch_active: self
                .download_lanes_yield_after_batch_active
                .load(Ordering::Relaxed),
            download_lanes_parking_active: self
                .download_lanes_parking_active
                .load(Ordering::Relaxed),
            download_lanes_recovering_active: self
                .download_lanes_recovering_active
                .load(Ordering::Relaxed),
            download_lane_parks_no_work_total: self
                .download_lane_parks_no_work_total
                .load(Ordering::Relaxed),
            download_lane_parks_pressure_total: self
                .download_lane_parks_pressure_total
                .load(Ordering::Relaxed),
            download_lane_parks_probe_yield_total: self
                .download_lane_parks_probe_yield_total
                .load(Ordering::Relaxed),
            download_lane_parks_error_total: self
                .download_lane_parks_error_total
                .load(Ordering::Relaxed),
            download_lane_lease_items_total: self
                .download_lane_lease_items_total
                .load(Ordering::Relaxed),
            download_lane_refill_granted_total: self
                .download_lane_refill_granted_total
                .load(Ordering::Relaxed),
            download_lane_refill_parked_total: self
                .download_lane_refill_parked_total
                .load(Ordering::Relaxed),
            download_lane_refill_deferred_total: self
                .download_lane_refill_deferred_total
                .load(Ordering::Relaxed),
            download_lane_refill_saturated_total: self
                .download_lane_refill_saturated_total
                .load(Ordering::Relaxed),
            download_pipeline_trial_success_total: self
                .download_pipeline_trial_success_total
                .load(Ordering::Relaxed),
            download_pipeline_trial_failure_total: self
                .download_pipeline_trial_failure_total
                .load(Ordering::Relaxed),
            download_pipeline_proof_pass_total: self
                .download_pipeline_proof_pass_total
                .load(Ordering::Relaxed),
            download_pipeline_cooldown_total: self
                .download_pipeline_cooldown_total
                .load(Ordering::Relaxed),
            download_pipeline_replay_items_total: self
                .download_pipeline_replay_items_total
                .load(Ordering::Relaxed),
            segments_downloaded,
            segments_decoded: self.segments_decoded.load(Ordering::Relaxed),
            segments_committed: self.segments_committed.load(Ordering::Relaxed),
            articles_not_found: self.articles_not_found.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            verify_active: self.verify_active.load(Ordering::Relaxed),
            repair_active: self.repair_active.load(Ordering::Relaxed),
            extract_active: self.extract_active.load(Ordering::Relaxed),
            disk_write_latency_us: self.disk_write_latency_us.load(Ordering::Relaxed),
            segments_retried: self.segments_retried.load(Ordering::Relaxed),
            segments_failed_permanent: self.segments_failed_permanent.load(Ordering::Relaxed),
            parked_infrastructure_work: self.parked_infrastructure_work.load(Ordering::Relaxed),
            nntp_generation_recovery_requeues: self
                .nntp_generation_recovery_requeues
                .load(Ordering::Relaxed),
            download_dispatch_stall_kicks: self
                .download_dispatch_stall_kicks
                .load(Ordering::Relaxed),
            download_dispatch_stall_releases: self
                .download_dispatch_stall_releases
                .load(Ordering::Relaxed),
            download_failures_article_not_found: self
                .download_failures_article_not_found
                .load(Ordering::Relaxed),
            download_failures_capacity_unavailable: self
                .download_failures_capacity_unavailable
                .load(Ordering::Relaxed),
            download_failures_transient: self.download_failures_transient.load(Ordering::Relaxed),
            download_failures_auth: self.download_failures_auth.load(Ordering::Relaxed),
            download_failures_permanent: self.download_failures_permanent.load(Ordering::Relaxed),
            current_download_speed,
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            recovery_queue_depth: self.recovery_queue_depth.load(Ordering::Relaxed),
            articles_per_sec: rates.articles,
            decode_rate_mbps: rates.decode_mib,
            par3: self.par3.snapshot(self.now_ms()),
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        self.sample_totals(
            Instant::now(),
            self.bytes_downloaded.load(Ordering::Relaxed),
            0,
            0,
        )
    }

    // The global download series as it stands before this tick, holding
    // nothing: what a job's download rate starts from, so the job's rate and
    // the gauge share every sample instant.
    pub(crate) fn download_rate_seed(&self) -> RateSeries {
        self.speed_tracker
            .lock()
            .unwrap()
            .bytes_downloaded
            .aligned_from(0)
    }

    // `downloaded` is the wire-byte counter the download gauge integrates. The
    // queue hands it the transfer total every job's own counter is credited
    // beside, so the jobs' download rates sum to the gauge.
    pub(crate) fn sample_totals(
        &self,
        now: Instant,
        downloaded: u64,
        decoded: u64,
        articles: u64,
    ) -> MetricsSnapshot {
        let bytes_downloaded = downloaded.max(self.bytes_downloaded.load(Ordering::Relaxed));
        let segments_downloaded = articles.max(self.segments_downloaded.load(Ordering::Relaxed));
        let bytes_decoded = decoded.max(self.bytes_decoded.load(Ordering::Relaxed));
        // One independent sampling tick advances every rate on the same clock.
        let rates = self.speed_tracker.lock().unwrap().update(
            now,
            downloaded,
            segments_downloaded,
            bytes_decoded,
        );
        let mut snapshot = self.snapshot_with_speed(bytes_downloaded, rates);
        snapshot.bytes_decoded = bytes_decoded;
        snapshot.segments_downloaded = segments_downloaded;
        snapshot
    }

    // Return a fresh atomics-based metrics snapshot without advancing the
    // shared speed tracker. The rate gauges carry the values the last real
    // tick computed, so an off-cadence reader sees the same live window rather
    // than a zero it would have to interpret as "stopped".
    pub fn raw_snapshot(&self) -> MetricsSnapshot {
        let bytes_downloaded = self.bytes_downloaded.load(Ordering::Relaxed);
        let rates = self.speed_tracker.lock().unwrap().last();
        self.snapshot_with_speed(bytes_downloaded, rates)
    }

    // How long this process has spent downloading, counted on the metrics
    // tick: only the intervals in which bytes arrived.
    pub fn download_time(&self) -> Duration {
        self.speed_tracker.lock().unwrap().download_time
    }
}

// Point-in-time snapshot of metrics (non-atomic, for reporting).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub bytes_downloaded: u64,
    pub bytes_decoded: u64,
    pub bytes_committed: u64,
    pub download_queue_depth: usize,
    pub active_downloads: usize,
    pub active_decodes: usize,
    pub decode_pending: usize,
    pub decode_pending_bytes: u64,
    pub decode_active_bytes: u64,
    pub commit_pending: usize,
    pub write_pending_bytes: u64,
    pub write_buffered_bytes: u64,
    pub write_buffered_segments: usize,
    pub uu_spooled_bytes: u64,
    pub uu_spooled_segments: usize,
    pub direct_write_evictions: u64,
    pub deobfuscated_members_renamed: u64,
    pub decode_pressure_soft_limit_bytes: u64,
    pub decode_pressure_hard_limit_bytes: u64,
    pub write_pressure_soft_limit_bytes: u64,
    pub write_pressure_hard_limit_bytes: u64,
    pub download_pressure_state: DownloadPressureState,
    pub download_pressure_reason: DownloadPressureReason,
    pub download_pressure_stalls_total: u64,
    pub download_pressure_stall_duration_ms: u64,
    pub download_pressure_current_stall_ms: u64,
    pub download_restart_durable_lead_blocked_total: u64,
    pub download_scheduler_idle_with_servable_total: u64,
    pub download_scheduler_handouts_total_hot: u64,
    pub download_scheduler_handouts_total_spill: u64,
    pub download_scheduler_handouts_total_probe: u64,
    #[serde(default)]
    pub download_scheduler_scan_items_skipped_total: u64,
    #[serde(default)]
    pub download_scheduler_scan_no_match_total: u64,
    // Indexed by [`SchedulerBlockClause::index`].
    #[serde(default)]
    pub download_scheduler_hot_blocked_total: [u64; SchedulerBlockClause::COUNT],
    pub download_lane_inflight_bytes: u64,
    pub download_jobs_eligible: usize,
    pub download_jobs_hot: usize,
    pub download_lanes_active: usize,
    pub download_lanes_sequential_active: usize,
    pub download_lanes_depth2_active: usize,
    pub download_lanes_depth4_active: usize,
    pub download_lanes_depth8_active: usize,
    pub download_lanes_idle_active: usize,
    pub download_lanes_awaiting_work_active: usize,
    pub download_lanes_binding_server_active: usize,
    pub download_lanes_acquired_active: usize,
    pub download_lanes_issuing_active: usize,
    pub download_lanes_draining_active: usize,
    pub download_lanes_yield_after_batch_active: usize,
    pub download_lanes_parking_active: usize,
    pub download_lanes_recovering_active: usize,
    pub download_lane_parks_no_work_total: u64,
    pub download_lane_parks_pressure_total: u64,
    pub download_lane_parks_probe_yield_total: u64,
    pub download_lane_parks_error_total: u64,
    pub download_lane_lease_items_total: u64,
    pub download_lane_refill_granted_total: u64,
    pub download_lane_refill_parked_total: u64,
    pub download_lane_refill_deferred_total: u64,
    pub download_lane_refill_saturated_total: u64,
    pub download_pipeline_trial_success_total: u64,
    pub download_pipeline_trial_failure_total: u64,
    pub download_pipeline_proof_pass_total: u64,
    pub download_pipeline_cooldown_total: u64,
    pub download_pipeline_replay_items_total: u64,
    pub segments_downloaded: u64,
    pub segments_decoded: u64,
    pub segments_committed: u64,
    pub articles_not_found: u64,
    pub decode_errors: u64,
    pub verify_active: usize,
    pub repair_active: usize,
    pub extract_active: usize,
    pub disk_write_latency_us: u64,
    pub segments_retried: u64,
    pub segments_failed_permanent: u64,
    #[serde(default)]
    pub parked_infrastructure_work: usize,
    #[serde(default)]
    pub nntp_generation_recovery_requeues: u64,
    #[serde(default)]
    pub download_dispatch_stall_kicks: u64,
    #[serde(default)]
    pub download_dispatch_stall_releases: u64,
    pub download_failures_article_not_found: u64,
    pub download_failures_capacity_unavailable: u64,
    pub download_failures_transient: u64,
    pub download_failures_auth: u64,
    pub download_failures_permanent: u64,
    pub current_download_speed: u64,
    pub crc_errors: u64,
    pub recovery_queue_depth: usize,
    pub articles_per_sec: f64,
    pub decode_rate_mbps: f64,
    // PAR3 recovery counters and the two work slots' phase/stall state.
    #[serde(default)]
    pub par3: Par3MetricsSnapshot,
}

#[cfg(test)]
mod tests;
