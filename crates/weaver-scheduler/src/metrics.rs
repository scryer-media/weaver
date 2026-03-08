use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Tracks download speed using a sliding window of byte samples.
struct SpeedTracker {
    /// Ring buffer of (timestamp, cumulative_bytes) samples.
    samples: Vec<(Instant, u64)>,
    /// Next write position in the ring buffer.
    pos: usize,
    /// Last computed speed (bytes/sec).
    speed: u64,
}

impl SpeedTracker {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(32),
            pos: 0,
            speed: 0,
        }
    }

    /// Record a sample and recompute speed. Called from `snapshot()`.
    fn update(&mut self, now: Instant, bytes_downloaded: u64) -> u64 {
        const WINDOW: usize = 20; // ~2 seconds at 100ms snapshot rate

        if self.samples.len() < WINDOW {
            self.samples.push((now, bytes_downloaded));
        } else {
            self.samples[self.pos] = (now, bytes_downloaded);
        }
        self.pos = (self.pos + 1) % WINDOW;

        // Compare newest sample to oldest in the window
        let newest = (now, bytes_downloaded);
        let oldest_idx = if self.samples.len() < WINDOW {
            0
        } else {
            self.pos % self.samples.len()
        };
        let oldest = self.samples[oldest_idx];

        let dt = newest.0.duration_since(oldest.0).as_secs_f64();
        if dt > 0.05 {
            let delta_bytes = newest.1.saturating_sub(oldest.1);
            self.speed = (delta_bytes as f64 / dt) as u64;
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
    pub decode_pending: AtomicUsize,
    pub commit_pending: AtomicUsize,

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

    // Speed — computed from bytes_downloaded delta
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
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn metrics_snapshot() {
        let m = PipelineMetrics::new();
        m.bytes_downloaded.store(1024, Ordering::Relaxed);
        m.segments_downloaded.store(5, Ordering::Relaxed);
        m.decode_pending.store(3, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.bytes_downloaded, 1024);
        assert_eq!(snap.segments_downloaded, 5);
        assert_eq!(snap.decode_pending, 3);
        assert_eq!(snap.bytes_decoded, 0);
    }

    #[tokio::test]
    async fn concurrent_metrics() {
        let m = PipelineMetrics::new();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let m = Arc::clone(&m);
            handles.push(tokio::spawn(async move {
                for _ in 0..1000 {
                    m.bytes_downloaded.fetch_add(100, Ordering::Relaxed);
                    m.segments_downloaded.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let snap = m.snapshot();
        assert_eq!(snap.bytes_downloaded, 10 * 1000 * 100);
        assert_eq!(snap.segments_downloaded, 10 * 1000);
    }
}
