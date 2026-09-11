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
    pub max_write_queue: usize,
    pub min_free_buffers: usize,
    pub decode_thread_count: usize,
    pub repair_thread_count: usize,
    pub extract_thread_count: usize,
}

/// The runtime tuner. Holds system profile and current parameters.
/// Adjusts parameters conservatively based on metrics.
pub struct RuntimeTuner {
    profile: SystemProfile,
    current: TunedParameters,
    max_concurrent_extractions_override: Option<usize>,
    /// Total connections across all configured servers (hard ceiling).
    total_connections: usize,
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

        // Every configured connection is a download connection. Memory
        // pressure changes where decoded bytes go (the write backlog spills
        // to disk), never how many articles are requested: a connection
        // count that ratchets down on pressure turns a transient backlog
        // into a lasting speed loss that only a restart undoes.
        let max_concurrent_downloads = total_connections;

        let repair_threads = cores.max(1);
        let extract_threads = (cores / 2).max(1);

        let current = TunedParameters {
            max_concurrent_downloads,
            max_write_queue: max_concurrent_downloads * 2,
            min_free_buffers: 4,
            decode_thread_count: cores,
            repair_thread_count: repair_threads,
            extract_thread_count: extract_threads,
        };

        Self {
            profile,
            current,
            max_concurrent_extractions_override,
            total_connections,
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

    /// Fold a metrics snapshot into the bandwidth estimate.
    ///
    /// The connection count is not adjusted here. It is the configured total,
    /// and only [`Self::set_connection_limit`] moves it.
    pub fn observe(&mut self, metrics: &MetricsSnapshot) {
        // EMA with α=0.3 gives ~15-second effective window at 5s intervals.
        //
        // No connections are held back for recovery work. Recovery blocks are
        // promoted into the job's own queue as completion-critical work and
        // ride the same lanes as the payload, so a reserved slot could only
        // ever be an idle connection: the reserve was subtracted from the
        // ordinary lease budget while the work it was reserved for was still
        // parked, waiting for a checkpoint that had not run yet.
        const ALPHA: f64 = 0.3;
        self.bandwidth_ema =
            ALPHA * metrics.current_download_speed as f64 + (1.0 - ALPHA) * self.bandwidth_ema;
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
        self.current.max_write_queue = limit * 2;
    }

    /// The system profile.
    pub fn profile(&self) -> &SystemProfile {
        &self.profile
    }

    /// Apply the asynchronous startup disk measurement without disrupting
    /// active work. Extraction admission reads this value on each promotion.
    pub fn set_random_read_iops(&mut self, random_read_iops: f64) {
        self.profile.disk.random_read_iops = random_read_iops;
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
