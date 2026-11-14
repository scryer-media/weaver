// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What admission reads of a volume's write rate. Read from what the disk
// owner already holds; nothing here looks at a disk. What is shown of each
// volume is its one row in `StorageCapacity::volume_snapshots`.

use std::path::Path;
use std::sync::atomic::Ordering;

use super::StorageCapacity;

impl StorageCapacity {
    // The live write rate of the volume `path` is on, in bytes per second;
    // zero until the volume has been measured.
    pub(crate) fn write_rate(&self, path: &Path) -> u64 {
        self.volume(path).write_rate.load(Ordering::Relaxed)
    }
}

// The slowest of the rates that have been measured, in MB/s (10^6 bytes).
// A rate of zero is a volume not measured yet and is passed over; when none
// has been measured the answer is zero, "no figure".
pub(crate) fn slowest_measured_mbps(rates: impl IntoIterator<Item = u64>) -> f64 {
    rates
        .into_iter()
        .filter(|rate| *rate > 0)
        .min()
        .unwrap_or(0) as f64
        / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    // Extraction admission takes the slowest volume that has a rate; a volume
    // not measured yet says nothing, and none measured is no figure.
    #[test]
    fn the_slowest_measured_rate_is_the_figure() {
        assert_eq!(slowest_measured_mbps([]), 0.0);
        assert_eq!(slowest_measured_mbps([0, 0]), 0.0);
        assert_eq!(slowest_measured_mbps([0, 250_000_000, 0]), 250.0);
        assert_eq!(slowest_measured_mbps([250_000_000, 60_000_000, 0]), 60.0);
    }
}
