// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the API shows of each volume: its label, the kind of storage it is,
// what it takes, what is being written to it now and what waits in memory
// for its writer. Read from
// what the disk owner already holds; nothing here looks at a disk.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::volume::Volume;
use super::{StorageCapacity, StorageKind};

// One volume as shown to the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeWriteRate {
    // The mount path or drive root.
    pub label: String,
    // "ssd", "platter", "network" or "unknown".
    pub storage_kind: &'static str,
    // Bytes per second the volume takes; `None` until measured.
    pub write_rate: Option<u64>,
    // Bytes per second written to it over the last window, whether or not
    // the volume was the limit; `None` when nothing was written.
    pub throughput: Option<u64>,
    // Bytes waiting in memory for the volume's writer.
    pub buffered_bytes: u64,
}

impl StorageCapacity {
    // Every volume the owner holds, by label. A volume's kind is the one the
    // metrics show for it: [`Self::kind_on_device`].
    pub fn volume_write_rates(&self) -> Vec<VolumeWriteRate> {
        self.write_rates_of(&self.volumes())
    }

    fn write_rates_of(&self, volumes: &[Arc<Volume>]) -> Vec<VolumeWriteRate> {
        write_rates(volumes, |device| self.kind_on_device(device))
    }

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

fn write_rates(
    volumes: &[Arc<Volume>],
    kind: impl Fn(Option<u64>) -> StorageKind,
) -> Vec<VolumeWriteRate> {
    let mut rates: Vec<VolumeWriteRate> = volumes
        .iter()
        .map(|volume| {
            let rate = volume.write_rate.load(Ordering::Relaxed);
            let throughput = volume.throughput.load(Ordering::Relaxed);
            VolumeWriteRate {
                label: volume.label.clone(),
                storage_kind: kind(volume.device).as_str(),
                write_rate: (rate > 0).then_some(rate),
                throughput: (throughput > 0).then_some(throughput),
                buffered_bytes: volume.buffered_bytes.load(Ordering::Relaxed),
            }
        })
        .collect();
    rates.sort_by(|a, b| a.label.cmp(&b.label));
    rates
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;

    fn volume(device: Option<u64>, label: &str, rate: u64, buffered: u64) -> Arc<Volume> {
        let volume = Volume::new(device, label.to_string());
        volume.write_rate.store(rate, Ordering::Relaxed);
        volume.buffered_bytes.store(buffered, Ordering::Relaxed);
        Arc::new(volume)
    }

    // A volume nothing has measured shows no rate, a measured one shows its
    // own, and each takes the kind held for its device.
    #[test]
    fn each_volume_shows_its_own_rate_and_kind() {
        let volumes = [
            volume(Some(2), "/mnt/pool", 180_000_000, 4096),
            volume(Some(1), "/", 0, 0),
            volume(None, "/mnt/share", 9_000_000, 0),
        ];
        // The pool is written to at a third of what it takes.
        volumes[0].throughput.store(60_000_000, Ordering::Relaxed);
        let kinds = HashMap::from([(1, StorageKind::Solid), (2, StorageKind::Rotational)]);

        let shown = write_rates(&volumes, |device| {
            device
                .and_then(|device| kinds.get(&device).copied())
                .unwrap_or(StorageKind::Unknown)
        });

        assert_eq!(
            shown,
            vec![
                VolumeWriteRate {
                    label: "/".into(),
                    storage_kind: "ssd",
                    write_rate: None,
                    throughput: None,
                    buffered_bytes: 0,
                },
                VolumeWriteRate {
                    label: "/mnt/pool".into(),
                    storage_kind: "platter",
                    write_rate: Some(180_000_000),
                    throughput: Some(60_000_000),
                    buffered_bytes: 4096,
                },
                VolumeWriteRate {
                    label: "/mnt/share".into(),
                    storage_kind: "unknown",
                    write_rate: Some(9_000_000),
                    throughput: None,
                    buffered_bytes: 0,
                },
            ]
        );
    }

    // Two roots lie on one device; the first could not tell its kind and the
    // second could. The list shows the kind that was told, as the metrics
    // row for the same volume does.
    #[test]
    fn a_volume_shows_the_kind_the_metrics_show_when_one_root_could_not_tell() {
        let dir = tempfile::tempdir().unwrap();
        let storage = StorageCapacity::with_probe(
            [
                dir.path().join("data"),
                dir.path().join("intermediate"),
                dir.path().join("complete"),
            ],
            Duration::from_secs(3600),
            Arc::new(|_| {
                Ok(super::super::DiskSpace {
                    total_bytes: 1,
                    available_bytes: 1,
                })
            }),
        );
        // Each root's own look is over before its reading is replaced, and
        // the next is an hour off.
        for (slot, kind) in [(0, StorageKind::Unknown), (1, StorageKind::Solid)] {
            storage.wait_for_probes(super::super::StorageRoot::ALL[slot], 1);
            let mut state = storage.slots[slot].lock();
            state.device = Some(Some(7));
            state.kind = Some(kind);
        }
        let volumes = [volume(Some(7), "/mnt/pool", 0, 0)];

        let shown = storage.write_rates_of(&volumes);

        assert_eq!(shown[0].storage_kind, "ssd");
        assert_eq!(
            shown[0].storage_kind,
            storage.kind_on_device(Some(7)).as_str()
        );
    }

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
