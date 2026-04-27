use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

const SPEED_WINDOW_SAMPLES: usize = 50; // ~5 seconds at 100ms snapshot rate
const SPEED_EMA_HALF_LIFE_SECS: f64 = 1.0;

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
    /// Raw article bodies waiting for decode scheduling or decode completion.
    pub decode_pending: AtomicUsize,
    pub commit_pending: AtomicUsize,
    pub write_buffered_bytes: AtomicU64,
    pub write_buffered_segments: AtomicUsize,
    pub direct_write_evictions: AtomicU64,

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
            decode_pending: AtomicUsize::new(0),
            commit_pending: AtomicUsize::new(0),
            write_buffered_bytes: AtomicU64::new(0),
            write_buffered_segments: AtomicUsize::new(0),
            direct_write_evictions: AtomicU64::new(0),
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
            speed_tracker: Mutex::new(SpeedTracker::new()),
            crc_errors: AtomicU64::new(0),
            recovery_queue_depth: AtomicUsize::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let elapsed = self.start_time.elapsed().as_secs_f64().max(0.001);
        let segments_downloaded = self.segments_downloaded.load(Ordering::Relaxed);
        let bytes_decoded = self.bytes_decoded.load(Ordering::Relaxed);
        let bytes_downloaded = self.bytes_downloaded.load(Ordering::Relaxed);

        let current_download_speed = self
            .speed_tracker
            .lock()
            .unwrap()
            .update(Instant::now(), bytes_downloaded);

        MetricsSnapshot {
            bytes_downloaded,
            bytes_decoded,
            bytes_committed: self.bytes_committed.load(Ordering::Relaxed),
            download_queue_depth: self.download_queue_depth.load(Ordering::Relaxed),
            decode_pending: self.decode_pending.load(Ordering::Relaxed),
            commit_pending: self.commit_pending.load(Ordering::Relaxed),
            write_buffered_bytes: self.write_buffered_bytes.load(Ordering::Relaxed),
            write_buffered_segments: self.write_buffered_segments.load(Ordering::Relaxed),
            direct_write_evictions: self.direct_write_evictions.load(Ordering::Relaxed),
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
            current_download_speed,
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
            recovery_queue_depth: self.recovery_queue_depth.load(Ordering::Relaxed),
            articles_per_sec: segments_downloaded as f64 / elapsed,
            decode_rate_mbps: (bytes_decoded as f64 / (1024.0 * 1024.0)) / elapsed,
        }
    }
}

/// Point-in-time snapshot of metrics (non-atomic, for reporting).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub bytes_downloaded: u64,
    pub bytes_decoded: u64,
    pub bytes_committed: u64,
    pub download_queue_depth: usize,
    pub decode_pending: usize,
    pub commit_pending: usize,
    pub write_buffered_bytes: u64,
    pub write_buffered_segments: usize,
    pub direct_write_evictions: u64,
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
    pub current_download_speed: u64,
    pub crc_errors: u64,
    pub recovery_queue_depth: usize,
    pub articles_per_sec: f64,
    pub decode_rate_mbps: f64,
}

#[cfg(test)]
mod tests;
