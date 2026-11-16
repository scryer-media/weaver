// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What admission reads of a volume's write rate. Read from what the disk
// owner already holds; nothing here looks at a disk. What is shown of each
// volume is its one row in `StorageCapacity::volume_snapshots`.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::Instant;

use super::StorageCapacity;
use super::volume::Volume;
use super::volumes::UNREAD_RETRY;

// The volumes of the paths last read for the slowest write rate, and what
// they were found under.
pub(super) struct RateVolumes {
    generation: u64,
    roots: Vec<PathBuf>,
    volumes: Vec<Arc<Volume>>,
    // When one of the paths is answered a volume of no known device: it was
    // not read yet (its read could not be queued or started, or is out), or
    // its read failed. The volumes are found again from then on, which asks
    // after such a path again. `None` once every device is known: nothing
    // reads a clock or looks up a path.
    retry_at: Option<Instant>,
}

impl StorageCapacity {
    // The slowest measured write rate, in MB/s, of the volumes `roots` and
    // `folders` lie on; `folders` are the ones last told (`set_folders`).
    // The volumes are found only when what a path answers may have changed
    // (`Known::generation`), and every `UNREAD_RETRY` while a path has no
    // known device (`RateVolumes::retry_at`): a turn that changed nothing
    // reads one rate per volume and looks up no path. A rate is read as it stands, so a sample
    // published since is in the answer.
    pub(crate) fn slowest_write_mbps(&self, roots: &[&Path], folders: &[PathBuf]) -> f64 {
        let generation = self.volumes.generation();
        let mut held = self
            .volumes
            .rate_volumes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let fresh = held.as_ref().is_some_and(|rates| {
            rates.generation == generation
                && rates.volumes.len() == roots.len() + folders.len()
                && rates.roots.len() == roots.len()
                && rates
                    .roots
                    .iter()
                    .zip(roots)
                    .all(|(held, root)| held.as_os_str() == root.as_os_str())
                && rates.retry_at.is_none_or(|at| Instant::now() < at)
        });
        if !fresh {
            let volumes: Vec<Arc<Volume>> = roots
                .iter()
                .copied()
                .chain(folders.iter().map(PathBuf::as_path))
                .map(|path| self.volume(path))
                .collect();
            let retry_at = volumes
                .iter()
                .any(|volume| volume.device.is_none())
                .then(|| Instant::now() + UNREAD_RETRY);
            *held = Some(RateVolumes {
                generation,
                roots: roots.iter().map(|root| root.to_path_buf()).collect(),
                volumes,
                retry_at,
            });
        }
        let rates = held.as_ref().expect("set above");
        slowest_measured_mbps(
            rates
                .volumes
                .iter()
                .map(|volume| volume.write_rate.load(Ordering::Relaxed)),
        )
    }
}

#[cfg(test)]
impl RateVolumes {
    // Whether every volume held has a known device.
    pub(super) fn settled(&self) -> bool {
        self.retry_at.is_none()
    }

    // Have the volumes found again on the next turn, as `UNREAD_RETRY`
    // after they were found would.
    pub(super) fn age(&mut self) {
        if self.retry_at.is_some() {
            self.retry_at = Some(Instant::now());
        }
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
