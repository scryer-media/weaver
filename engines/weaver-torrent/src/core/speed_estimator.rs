// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;

#[derive(Clone, Copy)]
struct ProgressSnapshot {
    progress_bytes: u64,
    instant: Instant,
}

/// Estimates download/upload speed in a sliding time window.
pub struct SpeedEstimator {
    latest_per_second_snapshots: Mutex<VecDeque<ProgressSnapshot>>,
    bytes_per_second: AtomicU64,
    time_remaining_millis: AtomicU64,
}

impl std::fmt::Debug for SpeedEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeedEstimator")
            .field("bps", &self.bps())
            .finish()
    }
}

impl Default for SpeedEstimator {
    fn default() -> Self {
        Self::new(5)
    }
}

impl SpeedEstimator {
    pub fn new(window_seconds: usize) -> Self {
        assert!(window_seconds > 1);
        Self {
            latest_per_second_snapshots: Mutex::new(VecDeque::with_capacity(window_seconds)),
            bytes_per_second: Default::default(),
            time_remaining_millis: Default::default(),
        }
    }

    pub fn time_remaining(&self) -> Option<Duration> {
        let tr = self.time_remaining_millis.load(Ordering::Relaxed);
        if tr == 0 {
            return None;
        }
        Some(Duration::from_millis(tr))
    }

    pub fn bps(&self) -> u64 {
        self.bytes_per_second.load(Ordering::Relaxed)
    }

    pub fn mbps(&self) -> f64 {
        self.bps() as f64 / 1024f64 / 1024f64
    }

    pub fn add_snapshot(
        &self,
        progress_bytes: u64,
        remaining_bytes: Option<u64>,
        instant: Instant,
    ) {
        let first = {
            let mut g = self.latest_per_second_snapshots.lock();

            let current = ProgressSnapshot {
                progress_bytes,
                instant,
            };

            if g.is_empty() {
                g.push_back(current);
                return;
            } else if g.len() < g.capacity() {
                g.push_back(current);
                g.front().copied().unwrap()
            } else {
                let first = g.pop_front().unwrap();
                g.push_back(current);
                first
            }
        };

        let downloaded_bytes_diff = progress_bytes - first.progress_bytes;
        let elapsed = (instant - first.instant).as_nanos();
        // Bytes over the window per second; as many as a u64 holds when no
        // time has passed.
        let bps = match u128::from(downloaded_bytes_diff)
            .saturating_mul(1_000_000_000)
            .checked_div(elapsed)
        {
            Some(bps) => u64::try_from(bps).unwrap_or(u64::MAX),
            None if downloaded_bytes_diff == 0 => 0,
            None => u64::MAX,
        };

        // The remaining bytes at the window's rate.
        let time_remaining_millis_rounded: u64 = if downloaded_bytes_diff > 0 {
            let millis = u128::from(remaining_bytes.unwrap_or_default()).saturating_mul(elapsed)
                / (u128::from(downloaded_bytes_diff) * 1_000_000);
            u64::try_from(millis).unwrap_or(u64::MAX)
        } else {
            0
        };
        self.time_remaining_millis
            .store(time_remaining_millis_rounded, Ordering::Relaxed);
        self.bytes_per_second.store(bps, Ordering::Relaxed);
    }
}
