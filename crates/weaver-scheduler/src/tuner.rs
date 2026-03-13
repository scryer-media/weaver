use serde::{Deserialize, Serialize};

use weaver_core::system::{StorageClass, SystemProfile};

use crate::metrics::MetricsSnapshot;

/// Runtime-tunable parameters. The tuner adjusts these based on observed performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunedParameters {
    pub max_concurrent_downloads: usize,
    pub max_decode_queue: usize,
    pub max_write_queue: usize,
    pub min_free_buffers: usize,
    pub decode_thread_count: usize,
    pub repair_thread_count: usize,
    pub extract_thread_count: usize,
    /// Max concurrent downloads for speculative PAR2 recovery blocks.
    /// Dynamically set based on observed bandwidth. 0 = hold back entirely.
    pub recovery_slots: usize,
}

/// The runtime tuner. Holds system profile and current parameters.
/// Adjusts parameters conservatively based on metrics.
pub struct RuntimeTuner {
    profile: SystemProfile,
    current: TunedParameters,
    /// Total connections across all configured servers (hard ceiling).
    total_connections: usize,
    /// Number of consecutive snapshots where decode_pending > max_decode_queue.
    decode_pressure_streak: u32,
    /// Number of consecutive snapshots where download_queue_depth == 0.
    download_idle_streak: u32,
    /// Number of consecutive snapshots where post-processing is active.
    pp_active_streak: u32,
    /// Saved max_concurrent_downloads before PP throttling (to restore after).
    pre_pp_max_downloads: Option<usize>,
    /// Exponential moving average of download speed (bytes/sec).
    bandwidth_ema: f64,
}

impl RuntimeTuner {
    /// Create with initial parameters derived from system profile.
    /// `total_connections` is the sum of all configured server connections —
    /// the tuner will never exceed this since the pool can't open more.
    pub fn new(profile: SystemProfile) -> Self {
        Self::with_connection_limit(profile, usize::MAX)
    }

    /// Create with an explicit connection limit (from config).
    pub fn with_connection_limit(profile: SystemProfile, total_connections: usize) -> Self {
        let cores = profile.cpu.physical_cores.max(1);

        // SSD: start with all configured connections — network latency is the
        // bottleneck, not disk. The tuner will reduce if decode can't keep up.
        // HDD: conservative start since random I/O contention hurts.
        let max_concurrent_downloads = match profile.disk.storage_class {
            StorageClass::Ssd => total_connections,
            _ => cores.min(total_connections).min(8),
        };

        let repair_threads = cores.max(1);
        let extract_threads = (cores / 2).max(1);

        let current = TunedParameters {
            max_concurrent_downloads,
            max_decode_queue: max_concurrent_downloads * 2,
            max_write_queue: max_concurrent_downloads * 2,
            min_free_buffers: 4,
            decode_thread_count: cores,
            repair_thread_count: repair_threads,
            extract_thread_count: extract_threads,
            recovery_slots: 0, // starts at 0 until bandwidth is observed
        };

        Self {
            profile,
            current,
            total_connections,
            decode_pressure_streak: 0,
            download_idle_streak: 0,
            pp_active_streak: 0,
            pre_pp_max_downloads: None,
            bandwidth_ema: 0.0,
        }
    }

    /// Get current tuned parameters.
    pub fn params(&self) -> &TunedParameters {
        &self.current
    }

    /// Adjust parameters based on a metrics snapshot.
    /// Returns true if any parameter changed.
    pub fn adjust(&mut self, metrics: &MetricsSnapshot) -> bool {
        let mut changed = false;

        // --- Bandwidth tracking & recovery slot computation ---
        // EMA with α=0.3 gives ~15-second effective window at 5s intervals.
        const ALPHA: f64 = 0.3;
        self.bandwidth_ema =
            ALPHA * metrics.current_download_speed as f64 + (1.0 - ALPHA) * self.bandwidth_ema;

        let bandwidth_mbps = self.bandwidth_ema / (1024.0 * 1024.0);
        let new_recovery_slots = if bandwidth_mbps > 50.0 {
            // High bandwidth: 25% of connections for speculative recovery
            (self.current.max_concurrent_downloads / 4).max(2)
        } else if bandwidth_mbps > 10.0 {
            // Medium bandwidth: trickle 1-2 connections
            (self.current.max_concurrent_downloads / 8).max(1)
        } else {
            // Low/no bandwidth: don't waste on speculative downloads
            0
        };
        if self.current.recovery_slots != new_recovery_slots {
            self.current.recovery_slots = new_recovery_slots;
            changed = true;
        }

        // --- Post-processing awareness ---
        let pp_active =
            metrics.verify_active > 0 || metrics.repair_active > 0 || metrics.extract_active > 0;

        if pp_active {
            self.pp_active_streak += 1;
        } else if self.pp_active_streak > 0 {
            // PP finished — restore download concurrency.
            if let Some(saved) = self.pre_pp_max_downloads.take() {
                self.current.max_concurrent_downloads = saved;
                changed = true;
            }
            self.pp_active_streak = 0;
        }

        // Throttle downloads during PP. HDD needs aggressive reduction (disk
        // contention between sequential writes and random reads kills throughput),
        // SSD barely needs any.
        const PP_STREAK_THRESHOLD: u32 = 2;
        if self.pp_active_streak == PP_STREAK_THRESHOLD {
            let baseline = self.max_downloads_limit();
            if self.pre_pp_max_downloads.is_none() {
                self.pre_pp_max_downloads = Some(self.current.max_concurrent_downloads);
            }
            let reduced = match self.profile.disk.storage_class {
                StorageClass::Hdd => (baseline / 4).max(1),
                StorageClass::Ssd => (baseline * 3 / 4).max(1),
                _ => (baseline / 2).max(1),
            };
            if self.current.max_concurrent_downloads != reduced {
                self.current.max_concurrent_downloads = reduced;
                changed = true;
            }
        }

        // --- Decode pressure / idle streaks ---
        if metrics.decode_pending > self.current.max_decode_queue {
            self.decode_pressure_streak += 1;
            self.download_idle_streak = 0;
        } else if metrics.download_queue_depth == 0 {
            self.download_idle_streak += 1;
            self.decode_pressure_streak = 0;
        } else {
            self.decode_pressure_streak = 0;
            self.download_idle_streak = 0;
        }

        const STREAK_THRESHOLD: u32 = 3;

        if self.decode_pressure_streak >= STREAK_THRESHOLD
            && self.current.max_concurrent_downloads > 1
        {
            self.current.max_concurrent_downloads -= 1;
            self.decode_pressure_streak = 0;
            return true;
        }

        if self.download_idle_streak >= STREAK_THRESHOLD && self.pre_pp_max_downloads.is_none() {
            let limit = self.max_downloads_limit();
            if self.current.max_concurrent_downloads < limit {
                self.current.max_concurrent_downloads += 1;
                self.download_idle_streak = 0;
                return true;
            }
        }

        changed
    }

    /// Upper limit for max_concurrent_downloads based on system profile
    /// and configured connection count.
    fn max_downloads_limit(&self) -> usize {
        match self.profile.disk.storage_class {
            StorageClass::Ssd => self.total_connections,
            _ => self
                .profile
                .cpu
                .physical_cores
                .min(self.total_connections)
                .min(8),
        }
    }

    /// Update the connection limit (e.g. after adding/removing a server) and
    /// recalculate `max_concurrent_downloads` so downloads can use the new capacity.
    pub fn set_connection_limit(&mut self, total_connections: usize) {
        self.total_connections = total_connections;
        let limit = self.max_downloads_limit();
        // Re-derive max_concurrent_downloads the same way as initial construction,
        // so adding a server immediately makes those connections available.
        self.current.max_concurrent_downloads = limit;
        self.current.max_decode_queue = limit * 2;
        self.current.max_write_queue = limit * 2;
    }

    /// The system profile.
    pub fn profile(&self) -> &SystemProfile {
        &self.profile
    }

    /// Maximum concurrent streaming member extractions, adaptive to disk type.
    ///
    /// SSD: no seek penalty, scale with CPU cores (2-6).
    /// HDD: seeking between concurrent read positions kills throughput (1-2).
    /// Network/Unknown: moderate (2).
    pub fn max_concurrent_extractions(&self) -> usize {
        match self.profile.disk.storage_class {
            StorageClass::Ssd => {
                // SSD: bottleneck is CPU decompression, not I/O
                let cores = self.profile.cpu.physical_cores;
                cores.clamp(2, 6)
            }
            StorageClass::Hdd => {
                // HDD: head seeks between concurrent streams are devastating.
                // Allow 2 only if IOPS suggests a decent drive.
                if self.profile.disk.random_read_iops > 500.0 {
                    2
                } else {
                    1
                }
            }
            _ => 2,
        }
    }

    /// Current bandwidth estimate (bytes/sec, exponential moving average).
    pub fn bandwidth_ema(&self) -> f64 {
        self.bandwidth_ema
    }
}

#[cfg(test)]
mod tests {
    use weaver_core::system::*;

    use super::*;

    fn ssd_profile(cores: usize) -> SystemProfile {
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: cores,
                logical_cores: cores * 2,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 16 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50000.0,
                same_filesystem: true,
            },
        }
    }

    fn hdd_profile(cores: usize) -> SystemProfile {
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: cores,
                logical_cores: cores * 2,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 16 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Hdd,
                filesystem: FilesystemType::Ext4,
                sequential_write_mbps: 150.0,
                random_read_iops: 200.0,
                same_filesystem: true,
            },
        }
    }

    fn empty_metrics() -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_downloaded: 0,
            bytes_decoded: 0,
            bytes_committed: 0,
            download_queue_depth: 0,
            decode_pending: 0,
            commit_pending: 0,
            write_buffered_bytes: 0,
            write_buffered_segments: 0,
            direct_write_evictions: 0,
            segments_downloaded: 0,
            segments_decoded: 0,
            segments_committed: 0,
            articles_not_found: 0,
            decode_errors: 0,
            verify_active: 0,
            repair_active: 0,
            extract_active: 0,
            disk_write_latency_us: 0,
            segments_retried: 0,
            segments_failed_permanent: 0,
            current_download_speed: 0,
            crc_errors: 0,
            recovery_queue_depth: 0,
            articles_per_sec: 0.0,
            decode_rate_mbps: 0.0,
        }
    }

    /// Standard test connection limit (mimics a typical config).
    const TEST_CONNECTIONS: usize = 20;

    #[test]
    fn initial_params_ssd() {
        let tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
        let p = tuner.params();
        assert_eq!(p.max_concurrent_downloads, 20); // SSD uses all connections
        assert_eq!(p.max_decode_queue, 40); // 20 * 2
        assert_eq!(p.max_write_queue, 40);
        assert_eq!(p.min_free_buffers, 4);
        assert_eq!(p.decode_thread_count, 8);
    }

    #[test]
    fn initial_params_hdd() {
        let tuner = RuntimeTuner::new(hdd_profile(8));
        let p = tuner.params();
        assert_eq!(p.max_concurrent_downloads, 8); // min(8, 8)
        assert_eq!(p.max_decode_queue, 16);
        assert_eq!(p.max_write_queue, 16);
        assert_eq!(p.decode_thread_count, 8);
    }

    #[test]
    fn adjust_reduces_downloads() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);
        let initial = tuner.params().max_concurrent_downloads;

        // Simulate decode pressure (decode_pending > max_decode_queue).
        let mut m = empty_metrics();
        m.decode_pending = tuner.params().max_decode_queue + 10;

        // Need 3 consecutive observations before adjustment.
        assert!(!tuner.adjust(&m));
        assert!(!tuner.adjust(&m));
        assert!(tuner.adjust(&m));
        assert_eq!(tuner.params().max_concurrent_downloads, initial - 1);
    }

    #[test]
    fn adjust_increases_downloads() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);

        // First reduce downloads so there's room to increase.
        let mut pressure = empty_metrics();
        pressure.decode_pending = tuner.params().max_decode_queue + 10;
        for _ in 0..3 {
            tuner.adjust(&pressure);
        }
        let reduced = tuner.params().max_concurrent_downloads;

        // Now simulate idle download queue.
        let mut idle = empty_metrics();
        idle.download_queue_depth = 0;

        assert!(!tuner.adjust(&idle));
        assert!(!tuner.adjust(&idle));
        assert!(tuner.adjust(&idle));
        assert_eq!(tuner.params().max_concurrent_downloads, reduced + 1);
    }

    #[test]
    fn adjust_respects_minimums() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(1), 2);
        // With 1 core, 2 connections on SSD: max_concurrent_downloads = 2.

        let mut pressure = empty_metrics();
        pressure.decode_pending = 1000;

        // Reduce from 2 to 1.
        for _ in 0..3 {
            tuner.adjust(&pressure);
        }
        assert_eq!(tuner.params().max_concurrent_downloads, 1);

        // Try to reduce further -- should stay at 1.
        for _ in 0..6 {
            tuner.adjust(&pressure);
        }
        assert_eq!(tuner.params().max_concurrent_downloads, 1);
    }

    #[test]
    fn pp_throttles_hdd_aggressively() {
        let mut tuner = RuntimeTuner::new(hdd_profile(8));
        // HDD with 8 cores: max_concurrent_downloads = 8.
        assert_eq!(tuner.params().max_concurrent_downloads, 8);

        let mut m = empty_metrics();
        m.extract_active = 1;

        // PP streak threshold is 2.
        tuner.adjust(&m);
        assert_eq!(tuner.params().max_concurrent_downloads, 8); // not yet
        tuner.adjust(&m);
        // HDD: reduced to baseline/4 = 8/4 = 2.
        assert_eq!(tuner.params().max_concurrent_downloads, 2);
    }

    #[test]
    fn pp_throttles_ssd_gently() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
        // SSD with 20 connections: max_concurrent_downloads = 20.
        assert_eq!(tuner.params().max_concurrent_downloads, 20);

        let mut m = empty_metrics();
        m.verify_active = 2;

        tuner.adjust(&m);
        tuner.adjust(&m);
        // SSD: reduced to baseline*3/4 = 20*3/4 = 15.
        assert_eq!(tuner.params().max_concurrent_downloads, 15);
    }

    #[test]
    fn pp_restores_after_completion() {
        let mut tuner = RuntimeTuner::new(hdd_profile(8));
        assert_eq!(tuner.params().max_concurrent_downloads, 8);

        // Trigger PP throttling.
        let mut pp = empty_metrics();
        pp.repair_active = 1;
        tuner.adjust(&pp);
        tuner.adjust(&pp);
        assert_eq!(tuner.params().max_concurrent_downloads, 2);

        // PP finishes.
        let idle = empty_metrics();
        tuner.adjust(&idle);
        // Should restore to pre-PP value.
        assert_eq!(tuner.params().max_concurrent_downloads, 8);
    }

    #[test]
    fn recovery_slots_high_bandwidth() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
        assert_eq!(tuner.params().recovery_slots, 0); // starts at 0

        // Simulate high bandwidth (60 MB/s = 62914560 bytes/sec).
        let mut m = empty_metrics();
        m.current_download_speed = 60 * 1024 * 1024;

        // EMA needs a few iterations to ramp up past 50 MB/s threshold.
        for _ in 0..10 {
            tuner.adjust(&m);
        }
        // 25% of 20 concurrent downloads = 5, max(5, 2) = 5.
        assert!(tuner.params().recovery_slots >= 2);
    }

    #[test]
    fn recovery_slots_medium_bandwidth() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);

        // Simulate medium bandwidth (20 MB/s).
        let mut m = empty_metrics();
        m.current_download_speed = 20 * 1024 * 1024;

        for _ in 0..10 {
            tuner.adjust(&m);
        }
        // Medium: max(20/8, 1) = max(2, 1) = 2.
        assert!(tuner.params().recovery_slots >= 1);
    }

    #[test]
    fn recovery_slots_low_bandwidth() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);

        // Simulate low bandwidth (5 MB/s).
        let mut m = empty_metrics();
        m.current_download_speed = 5 * 1024 * 1024;

        for _ in 0..10 {
            tuner.adjust(&m);
        }
        assert_eq!(tuner.params().recovery_slots, 0);
    }

    #[test]
    fn no_idle_increase_during_pp() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);
        // Start: max_concurrent_downloads = 20.

        // Reduce via decode pressure first.
        let mut pressure = empty_metrics();
        pressure.decode_pending = tuner.params().max_decode_queue + 10;
        for _ in 0..3 {
            tuner.adjust(&pressure);
        }
        let reduced = tuner.params().max_concurrent_downloads;

        // Now PP is active with empty download queue — should NOT increase.
        let mut pp_idle = empty_metrics();
        pp_idle.extract_active = 1;
        pp_idle.download_queue_depth = 0;
        for _ in 0..5 {
            tuner.adjust(&pp_idle);
        }
        // Should be PP-throttled, not increased.
        assert!(tuner.params().max_concurrent_downloads <= reduced);
    }

    #[test]
    fn set_connection_limit_increases_capacity() {
        // Start with 0 connections (no servers configured).
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 0);
        assert_eq!(tuner.params().max_concurrent_downloads, 0);

        // Add a server with 20 connections.
        tuner.set_connection_limit(20);
        assert_eq!(tuner.params().max_concurrent_downloads, 20); // SSD uses all
        assert_eq!(tuner.params().max_decode_queue, 40);
    }

    #[test]
    fn set_connection_limit_decreases_capacity() {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 20);
        assert_eq!(tuner.params().max_concurrent_downloads, 20);

        // Remove a server, now only 5 connections.
        tuner.set_connection_limit(5);
        assert_eq!(tuner.params().max_concurrent_downloads, 5);
    }
}
