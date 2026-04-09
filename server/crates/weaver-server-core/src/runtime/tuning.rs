use std::env;

use serde::{Deserialize, Serialize};

use crate::runtime::system_profile::SystemProfile;

use crate::operations::metrics::MetricsSnapshot;

/// IOPS threshold for "fast" storage (SSD/NVMe). Above this, disk is not the
/// bottleneck and we can use all configured connections. Below this, we
/// throttle to avoid disk contention.
const FAST_STORAGE_IOPS: f64 = 1_000.0;
const MAX_CONCURRENT_EXTRACTIONS_ENV: &str = "WEAVER_MAX_CONCURRENT_EXTRACTIONS";

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
    max_concurrent_extractions_override: Option<usize>,
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
        let max_concurrent_extractions_override = parse_max_concurrent_extractions_override(
            env::var(MAX_CONCURRENT_EXTRACTIONS_ENV).ok().as_deref(),
        );

        // Start with all configured connections — the adaptive tuner will
        // reduce if decode pressure builds or disk can't keep up. Never cap
        // below what the user configured; if throttling is needed, the
        // decode_pressure_streak mechanism handles it automatically.
        let max_concurrent_downloads = total_connections;

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
            max_concurrent_extractions_override,
            total_connections,
            decode_pressure_streak: 0,
            download_idle_streak: 0,
            pp_active_streak: 0,
            pre_pp_max_downloads: None,
            bandwidth_ema: 0.0,
        }
    }

    /// Whether the detected storage is fast enough that disk I/O is not the
    /// bottleneck (SSD/NVMe). Based on measured IOPS, not storage class enum,
    /// so it works correctly in Docker and other environments where
    /// `/sys/block` detection fails.
    fn is_fast_storage(&self) -> bool {
        self.profile.disk.random_read_iops >= FAST_STORAGE_IOPS
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
            let reduced = if self.is_fast_storage() {
                (baseline * 3 / 4).max(1)
            } else {
                (baseline / 4).max(1)
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
        self.total_connections
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
        if let Some(override_value) = self.max_concurrent_extractions_override {
            return override_value;
        }
        if self.is_fast_storage() {
            // Fast storage: bottleneck is CPU decompression, not I/O.
            let cores = self.profile.cpu.physical_cores;
            cores.clamp(2, 6)
        } else {
            // Slow storage: head seeks between concurrent streams hurt.
            // Allow 2 only if IOPS suggests a decent drive.
            if self.profile.disk.random_read_iops > 500.0 {
                2
            } else {
                1
            }
        }
    }

    /// Current bandwidth estimate (bytes/sec, exponential moving average).
    pub fn bandwidth_ema(&self) -> f64 {
        self.bandwidth_ema
    }
}

fn parse_max_concurrent_extractions_override(raw: Option<&str>) -> Option<usize> {
    let value = raw?.trim();
    if value.is_empty() {
        return None;
    }
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 1 => Some(parsed),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
