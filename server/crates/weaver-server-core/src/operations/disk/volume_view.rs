// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the API shows of each volume: its label, the kind of storage it is,
// its live write rate and what waits in memory for its writer. Read from
// what the disk owner already holds; nothing here looks at a disk.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::volume::Volume;
use super::{StorageCapacity, StorageKind, lock_paths};

// One volume as shown to the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeWriteRate {
    // The mount path or drive root.
    pub label: String,
    // "ssd", "platter", "network" or "unknown".
    pub storage_kind: &'static str,
    // Bytes per second the volume takes; `None` until measured.
    pub write_rate: Option<u64>,
    // Bytes waiting in memory for the volume's writer.
    pub buffered_bytes: u64,
}

impl StorageCapacity {
    // Every volume the owner holds, by label. A volume's kind is the kind
    // held for any root or folder on the same device, and unknown when none
    // is held or the volume has no device.
    pub fn volume_write_rates(&self) -> Vec<VolumeWriteRate> {
        let volumes = self.volumes();
        if volumes.is_empty() {
            return Vec::new();
        }
        write_rates(&volumes, &self.kinds_by_device())
    }

    // The live write rate of the volume `path` is on, in MB/s (10^6 bytes);
    // zero until the volume has been measured.
    pub(crate) fn write_mbps(&self, path: &Path) -> f64 {
        mbps(&self.volume(path))
    }

    // The kind of storage held for each device: a root's first, then a
    // folder's from its latest look. One pass over the three roots and the
    // bounded folder cache.
    fn kinds_by_device(&self) -> HashMap<u64, StorageKind> {
        let mut kinds = HashMap::new();
        for slot in &self.slots {
            let state = slot.lock();
            if let (Some(Some(device)), Some(kind)) = (state.device, state.kind) {
                kinds.entry(device).or_insert(kind);
            }
        }
        for entry in lock_paths(&self.paths).entries.values() {
            if let (Some(device), Some(space)) = (
                entry.device.as_ref().and_then(|look| look.device),
                entry.space.as_ref(),
            ) {
                kinds.entry(device).or_insert(space.kind);
            }
        }
        kinds
    }
}

fn mbps(volume: &Volume) -> f64 {
    volume.write_rate.load(Ordering::Relaxed) as f64 / 1_000_000.0
}

fn write_rates(
    volumes: &[Arc<Volume>],
    kinds: &HashMap<u64, StorageKind>,
) -> Vec<VolumeWriteRate> {
    let mut rates: Vec<VolumeWriteRate> = volumes
        .iter()
        .map(|volume| {
            let rate = volume.write_rate.load(Ordering::Relaxed);
            VolumeWriteRate {
                label: volume.label.clone(),
                storage_kind: volume
                    .device
                    .and_then(|device| kinds.get(&device).copied())
                    .unwrap_or(StorageKind::Unknown)
                    .as_str(),
                write_rate: (rate > 0).then_some(rate),
                buffered_bytes: volume.buffered_bytes.load(Ordering::Relaxed),
            }
        })
        .collect();
    rates.sort_by(|a, b| a.label.cmp(&b.label));
    rates
}

#[cfg(test)]
mod tests {
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
        let kinds = HashMap::from([(1, StorageKind::Solid), (2, StorageKind::Rotational)]);

        let shown = write_rates(&volumes, &kinds);

        assert_eq!(
            shown,
            vec![
                VolumeWriteRate {
                    label: "/".into(),
                    storage_kind: "ssd",
                    write_rate: None,
                    buffered_bytes: 0,
                },
                VolumeWriteRate {
                    label: "/mnt/pool".into(),
                    storage_kind: "platter",
                    write_rate: Some(180_000_000),
                    buffered_bytes: 4096,
                },
                VolumeWriteRate {
                    label: "/mnt/share".into(),
                    storage_kind: "unknown",
                    write_rate: Some(9_000_000),
                    buffered_bytes: 0,
                },
            ]
        );
    }

    // The tuner's figure is the volume's rate in MB/s, and zero, "not
    // measured", until the volume has one.
    #[test]
    fn a_volume_rate_reads_as_megabytes_per_second() {
        assert_eq!(mbps(&volume(Some(1), "/", 0, 0)), 0.0);
        assert_eq!(mbps(&volume(Some(1), "/", 60_000_000, 0)), 60.0);
    }
}
