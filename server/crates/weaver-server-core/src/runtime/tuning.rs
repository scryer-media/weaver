use std::env;

use serde::{Deserialize, Serialize};

use crate::runtime::system_profile::SystemProfile;

/// IOPS threshold for "fast" storage (SSD/NVMe). Above this, disk is not the
/// bottleneck and we can use all configured connections. Below this, we
/// throttle to avoid disk contention.
const FAST_STORAGE_IOPS: f64 = 1_000.0;
const MAX_CONCURRENT_EXTRACTIONS_ENV: &str = "WEAVER_MAX_CONCURRENT_EXTRACTIONS";

/// Runtime limits derived from the system profile and the configured servers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunedParameters {
    pub max_concurrent_downloads: usize,
    pub decode_thread_count: usize,
    pub extract_thread_count: usize,
}

/// Holds the system profile and the limits derived from it. The only value
/// that moves at runtime is the extraction concurrency, which follows the
/// measured disk once the startup benchmark lands.
pub struct RuntimeTuner {
    profile: SystemProfile,
    current: TunedParameters,
    max_concurrent_extractions_override: Option<usize>,
    /// Total connections across all configured servers (hard ceiling).
    total_connections: usize,
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
        let extract_threads = (cores / 2).max(1);

        let current = TunedParameters {
            max_concurrent_downloads,
            decode_thread_count: cores,
            extract_thread_count: extract_threads,
        };

        Self {
            profile,
            current,
            max_concurrent_extractions_override,
            total_connections,
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
