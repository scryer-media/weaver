use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

const SPEED_WINDOW_SAMPLES: usize = 50; // ~5 seconds at 100ms snapshot rate
const SPEED_EMA_HALF_LIFE_SECS: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadPressureState {
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

impl Default for DownloadPressureState {
    fn default() -> Self {
        Self::Clear
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadPressureReason {
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

impl Default for DownloadPressureReason {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchShareMode {
    Exclusive,
    Shared,
}

impl DispatchShareMode {
    pub const fn as_code(self) -> usize {
        match self {
            Self::Exclusive => 0,
            Self::Shared => 1,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exclusive => "exclusive",
            Self::Shared => "shared",
        }
    }

    pub const fn from_code(code: usize) -> Self {
        match code {
            1 => Self::Shared,
            _ => Self::Exclusive,
        }
    }
}

impl Default for DispatchShareMode {
    fn default() -> Self {
        Self::Exclusive
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpilloverDecision {
    None,
    BlockedWarmup,
    BlockedPressure,
    BlockedNearCap,
    BlockedHotCanUseCapacity,
    AllowedUnderfill,
    Reclaimed,
}

impl SpilloverDecision {
    pub const fn as_code(self) -> usize {
        match self {
            Self::None => 0,
            Self::BlockedWarmup => 1,
            Self::BlockedPressure => 2,
            Self::BlockedNearCap => 3,
            Self::BlockedHotCanUseCapacity => 4,
            Self::AllowedUnderfill => 5,
            Self::Reclaimed => 6,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BlockedWarmup => "blocked_warmup",
            Self::BlockedPressure => "blocked_pressure",
            Self::BlockedNearCap => "blocked_near_cap",
            Self::BlockedHotCanUseCapacity => "blocked_hot_can_use_capacity",
            Self::AllowedUnderfill => "allowed_underfill",
            Self::Reclaimed => "reclaimed",
        }
    }

    pub const fn from_code(code: usize) -> Self {
        match code {
            1 => Self::BlockedWarmup,
            2 => Self::BlockedPressure,
            3 => Self::BlockedNearCap,
            4 => Self::BlockedHotCanUseCapacity,
            5 => Self::AllowedUnderfill,
            6 => Self::Reclaimed,
            _ => Self::None,
        }
    }
}

impl Default for SpilloverDecision {
    fn default() -> Self {
        Self::None
    }
}

/// Tracks download speed using a sliding window of byte samples.
struct SpeedTracker {
    /// Ring buffer of (timestamp, cumulative_bytes) samples.
    samples: Vec<(Instant, u64)>,
    /// Next write position in the ring buffer.
    pos: usize,
    /// Last computed EMA-smoothed speed (bytes/sec).
    speed: u64,
    /// Floating-point accumulator used by the EMA.
    ema_speed: f64,
    /// Timestamp of the last EMA update.
    last_ema_at: Option<Instant>,
}

impl SpeedTracker {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(SPEED_WINDOW_SAMPLES),
            pos: 0,
            speed: 0,
            ema_speed: 0.0,
            last_ema_at: None,
        }
    }

    /// Record a sample and recompute speed on the pipeline metrics tick.
    fn update(&mut self, now: Instant, bytes_downloaded: u64) -> u64 {
        if self.samples.len() < SPEED_WINDOW_SAMPLES {
            self.samples.push((now, bytes_downloaded));
        } else {
            self.samples[self.pos] = (now, bytes_downloaded);
        }
        self.pos = (self.pos + 1) % SPEED_WINDOW_SAMPLES;

        // Compare newest sample to oldest in the current window, then smooth
        // the raw rate with a 1s half-life EMA so the published metric follows
        // pipeline ticks without showing every short-lived burst.
        let newest = (now, bytes_downloaded);
        let oldest_idx = if self.samples.len() < SPEED_WINDOW_SAMPLES {
            0
        } else {
            self.pos % self.samples.len()
        };
        let oldest = self.samples[oldest_idx];

        let dt = newest.0.duration_since(oldest.0).as_secs_f64();
        if dt > 0.05 {
            let delta_bytes = newest.1.saturating_sub(oldest.1);
            let raw_speed = delta_bytes as f64 / dt;

            if let Some(last_ema_at) = self.last_ema_at {
                let elapsed = now.duration_since(last_ema_at).as_secs_f64();
                if elapsed > 0.0 {
                    let alpha = 1.0 - 0.5_f64.powf(elapsed / SPEED_EMA_HALF_LIFE_SECS);
                    self.ema_speed += alpha * (raw_speed - self.ema_speed);
                } else {
                    self.ema_speed = raw_speed;
                }
            } else {
                self.ema_speed = raw_speed;
            }

            self.last_ema_at = Some(now);
            if self.ema_speed < 1.0 {
                self.ema_speed = 0.0;
            }
            self.speed = self.ema_speed as u64;
        }
        self.speed
    }
}

impl std::fmt::Debug for SpeedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeedTracker")
            .field("speed", &self.speed)
            .finish()
    }
}

/// Live metrics for the pipeline, updated atomically by various stages.
#[derive(Debug)]
pub struct PipelineMetrics {
    // Throughput
    pub bytes_downloaded: AtomicU64,
    pub bytes_decoded: AtomicU64,
    pub bytes_committed: AtomicU64,

    // Queue depths
    pub download_queue_depth: AtomicUsize,
    pub active_downloads: AtomicUsize,
    pub active_decodes: AtomicUsize,
    /// Raw article bodies waiting for decode scheduling.
    pub decode_pending: AtomicUsize,
    pub decode_pending_bytes: AtomicU64,
    pub decode_active_bytes: AtomicU64,
    pub commit_pending: AtomicUsize,
    pub write_buffered_bytes: AtomicU64,
    pub write_buffered_segments: AtomicUsize,
    pub direct_write_evictions: AtomicU64,
    pub decode_pressure_soft_limit_bytes: AtomicU64,
    pub decode_pressure_hard_limit_bytes: AtomicU64,
    pub write_pressure_soft_limit_bytes: AtomicU64,
    pub write_pressure_hard_limit_bytes: AtomicU64,
    pub download_pressure_state: AtomicUsize,
    pub download_pressure_reason: AtomicUsize,
    pub download_pressure_stalls_total: AtomicU64,
    pub download_pressure_stall_duration_ms: AtomicU64,
    pub download_pressure_current_stall_ms: AtomicU64,
    pub hot_dispatch_job_id: AtomicU64,
    pub hot_dispatch_mode: AtomicUsize,
    pub hot_dispatch_underfill_ms: AtomicU64,
    pub hot_dispatch_lent_connections: AtomicUsize,
    pub hot_dispatch_warmup_complete: AtomicUsize,
    pub hot_dispatch_last_spillover_decision: AtomicUsize,
    pub hot_dispatch_spillover_blocked_warmup_total: AtomicU64,
    pub hot_dispatch_spillover_blocked_pressure_total: AtomicU64,
    pub hot_dispatch_spillover_blocked_near_cap_total: AtomicU64,
    pub hot_dispatch_spillover_blocked_hot_can_use_capacity_total: AtomicU64,
    pub hot_dispatch_spillover_allowed_underfill_total: AtomicU64,
    pub hot_dispatch_spillover_reclaimed_total: AtomicU64,
    pub download_lanes_active: AtomicUsize,
    pub download_lanes_sequential_active: AtomicUsize,
    pub download_lanes_depth2_active: AtomicUsize,
    pub download_lanes_depth4_active: AtomicUsize,
    pub download_lane_parks_no_work_total: AtomicU64,
    pub download_lane_parks_error_total: AtomicU64,
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
    pub decode_errors: AtomicU64,

    // Post-processing activity
    pub verify_active: AtomicUsize,
    pub repair_active: AtomicUsize,
    pub extract_active: AtomicUsize,
    pub disk_write_latency_us: AtomicU64,

    // Retry tracking
    pub segments_retried: AtomicU64,
    pub segments_failed_permanent: AtomicU64,
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

    // Timing (not atomic — set once at creation)
    pub start_time: Instant,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes_downloaded: AtomicU64::new(0),
            bytes_decoded: AtomicU64::new(0),
            bytes_committed: AtomicU64::new(0),
            download_queue_depth: AtomicUsize::new(0),
            active_downloads: AtomicUsize::new(0),
            active_decodes: AtomicUsize::new(0),
            decode_pending: AtomicUsize::new(0),
            decode_pending_bytes: AtomicU64::new(0),
            decode_active_bytes: AtomicU64::new(0),
            commit_pending: AtomicUsize::new(0),
            write_buffered_bytes: AtomicU64::new(0),
            write_buffered_segments: AtomicUsize::new(0),
            direct_write_evictions: AtomicU64::new(0),
            decode_pressure_soft_limit_bytes: AtomicU64::new(0),
            decode_pressure_hard_limit_bytes: AtomicU64::new(0),
            write_pressure_soft_limit_bytes: AtomicU64::new(0),
            write_pressure_hard_limit_bytes: AtomicU64::new(0),
            download_pressure_state: AtomicUsize::new(DownloadPressureState::Clear.as_code()),
            download_pressure_reason: AtomicUsize::new(DownloadPressureReason::None.as_code()),
            download_pressure_stalls_total: AtomicU64::new(0),
            download_pressure_stall_duration_ms: AtomicU64::new(0),
            download_pressure_current_stall_ms: AtomicU64::new(0),
            hot_dispatch_job_id: AtomicU64::new(0),
            hot_dispatch_mode: AtomicUsize::new(DispatchShareMode::Exclusive.as_code()),
            hot_dispatch_underfill_ms: AtomicU64::new(0),
            hot_dispatch_lent_connections: AtomicUsize::new(0),
            hot_dispatch_warmup_complete: AtomicUsize::new(0),
            hot_dispatch_last_spillover_decision: AtomicUsize::new(
                SpilloverDecision::None.as_code(),
            ),
            hot_dispatch_spillover_blocked_warmup_total: AtomicU64::new(0),
            hot_dispatch_spillover_blocked_pressure_total: AtomicU64::new(0),
            hot_dispatch_spillover_blocked_near_cap_total: AtomicU64::new(0),
            hot_dispatch_spillover_blocked_hot_can_use_capacity_total: AtomicU64::new(0),
            hot_dispatch_spillover_allowed_underfill_total: AtomicU64::new(0),
            hot_dispatch_spillover_reclaimed_total: AtomicU64::new(0),
            download_lanes_active: AtomicUsize::new(0),
            download_lanes_sequential_active: AtomicUsize::new(0),
            download_lanes_depth2_active: AtomicUsize::new(0),
            download_lanes_depth4_active: AtomicUsize::new(0),
            download_lane_parks_no_work_total: AtomicU64::new(0),
            download_lane_parks_error_total: AtomicU64::new(0),
            download_pipeline_trial_success_total: AtomicU64::new(0),
            download_pipeline_trial_failure_total: AtomicU64::new(0),
            download_pipeline_proof_pass_total: AtomicU64::new(0),
            download_pipeline_cooldown_total: AtomicU64::new(0),
            download_pipeline_replay_items_total: AtomicU64::new(0),
            segments_downloaded: AtomicU64::new(0),
            segments_decoded: AtomicU64::new(0),
            segments_committed: AtomicU64::new(0),
            articles_not_found: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            verify_active: AtomicUsize::new(0),
            repair_active: AtomicUsize::new(0),
            extract_active: AtomicUsize::new(0),
            disk_write_latency_us: AtomicU64::new(0),
            segments_retried: AtomicU64::new(0),
            segments_failed_permanent: AtomicU64::new(0),
            download_failures_article_not_found: AtomicU64::new(0),
            download_failures_capacity_unavailable: AtomicU64::new(0),
            download_failures_transient: AtomicU64::new(0),
            download_failures_auth: AtomicU64::new(0),
            download_failures_permanent: AtomicU64::new(0),
            speed_tracker: Mutex::new(SpeedTracker::new()),
            crc_errors: AtomicU64::new(0),
            recovery_queue_depth: AtomicUsize::new(0),
            start_time: Instant::now(),
        })
    }

    fn saturating_sub_u64(counter: &AtomicU64, amount: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(amount))
        });
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

    fn snapshot_with_speed(
        &self,
        bytes_downloaded: u64,
        current_download_speed: u64,
    ) -> MetricsSnapshot {
        let elapsed = self.start_time.elapsed().as_secs_f64().max(0.001);
        let segments_downloaded = self.segments_downloaded.load(Ordering::Relaxed);
        let bytes_decoded = self.bytes_decoded.load(Ordering::Relaxed);

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
            write_buffered_bytes: self.write_buffered_bytes.load(Ordering::Relaxed),
            write_buffered_segments: self.write_buffered_segments.load(Ordering::Relaxed),
            direct_write_evictions: self.direct_write_evictions.load(Ordering::Relaxed),
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
            hot_dispatch_job_id: self.hot_dispatch_job_id.load(Ordering::Relaxed),
            hot_dispatch_mode: DispatchShareMode::from_code(
                self.hot_dispatch_mode.load(Ordering::Relaxed),
            ),
            hot_dispatch_underfill_ms: self.hot_dispatch_underfill_ms.load(Ordering::Relaxed),
            hot_dispatch_lent_connections: self
                .hot_dispatch_lent_connections
                .load(Ordering::Relaxed),
            hot_dispatch_warmup_complete: self.hot_dispatch_warmup_complete.load(Ordering::Relaxed)
                != 0,
            hot_dispatch_last_spillover_decision: SpilloverDecision::from_code(
                self.hot_dispatch_last_spillover_decision
                    .load(Ordering::Relaxed),
            ),
            hot_dispatch_spillover_blocked_warmup_total: self
                .hot_dispatch_spillover_blocked_warmup_total
                .load(Ordering::Relaxed),
            hot_dispatch_spillover_blocked_pressure_total: self
                .hot_dispatch_spillover_blocked_pressure_total
                .load(Ordering::Relaxed),
            hot_dispatch_spillover_blocked_near_cap_total: self
                .hot_dispatch_spillover_blocked_near_cap_total
                .load(Ordering::Relaxed),
            hot_dispatch_spillover_blocked_hot_can_use_capacity_total: self
                .hot_dispatch_spillover_blocked_hot_can_use_capacity_total
                .load(Ordering::Relaxed),
            hot_dispatch_spillover_allowed_underfill_total: self
                .hot_dispatch_spillover_allowed_underfill_total
                .load(Ordering::Relaxed),
            hot_dispatch_spillover_reclaimed_total: self
                .hot_dispatch_spillover_reclaimed_total
                .load(Ordering::Relaxed),
            download_lanes_active: self.download_lanes_active.load(Ordering::Relaxed),
            download_lanes_sequential_active: self
                .download_lanes_sequential_active
                .load(Ordering::Relaxed),
            download_lanes_depth2_active: self.download_lanes_depth2_active.load(Ordering::Relaxed),
            download_lanes_depth4_active: self.download_lanes_depth4_active.load(Ordering::Relaxed),
            download_lane_parks_no_work_total: self
                .download_lane_parks_no_work_total
                .load(Ordering::Relaxed),
            download_lane_parks_error_total: self
                .download_lane_parks_error_total
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
            articles_per_sec: segments_downloaded as f64 / elapsed,
            decode_rate_mbps: (bytes_decoded as f64 / (1024.0 * 1024.0)) / elapsed,
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let bytes_downloaded = self.bytes_downloaded.load(Ordering::Relaxed);
        let current_download_speed = self
            .speed_tracker
            .lock()
            .unwrap()
            .update(Instant::now(), bytes_downloaded);
        self.snapshot_with_speed(bytes_downloaded, current_download_speed)
    }

    /// Return a fresh atomics-based metrics snapshot without advancing the
    /// shared speed tracker. `current_download_speed` is left at zero so
    /// callers can derive it from their own sampling cadence when needed.
    pub fn raw_snapshot(&self) -> MetricsSnapshot {
        let bytes_downloaded = self.bytes_downloaded.load(Ordering::Relaxed);
        self.snapshot_with_speed(bytes_downloaded, 0)
    }
}

/// Point-in-time snapshot of metrics (non-atomic, for reporting).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub write_buffered_bytes: u64,
    pub write_buffered_segments: usize,
    pub direct_write_evictions: u64,
    pub decode_pressure_soft_limit_bytes: u64,
    pub decode_pressure_hard_limit_bytes: u64,
    pub write_pressure_soft_limit_bytes: u64,
    pub write_pressure_hard_limit_bytes: u64,
    pub download_pressure_state: DownloadPressureState,
    pub download_pressure_reason: DownloadPressureReason,
    pub download_pressure_stalls_total: u64,
    pub download_pressure_stall_duration_ms: u64,
    pub download_pressure_current_stall_ms: u64,
    pub hot_dispatch_job_id: u64,
    pub hot_dispatch_mode: DispatchShareMode,
    pub hot_dispatch_underfill_ms: u64,
    pub hot_dispatch_lent_connections: usize,
    pub hot_dispatch_warmup_complete: bool,
    pub hot_dispatch_last_spillover_decision: SpilloverDecision,
    pub hot_dispatch_spillover_blocked_warmup_total: u64,
    pub hot_dispatch_spillover_blocked_pressure_total: u64,
    pub hot_dispatch_spillover_blocked_near_cap_total: u64,
    pub hot_dispatch_spillover_blocked_hot_can_use_capacity_total: u64,
    pub hot_dispatch_spillover_allowed_underfill_total: u64,
    pub hot_dispatch_spillover_reclaimed_total: u64,
    pub download_lanes_active: usize,
    pub download_lanes_sequential_active: usize,
    pub download_lanes_depth2_active: usize,
    pub download_lanes_depth4_active: usize,
    pub download_lane_parks_no_work_total: u64,
    pub download_lane_parks_error_total: u64,
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
}

#[cfg(test)]
mod tests;
