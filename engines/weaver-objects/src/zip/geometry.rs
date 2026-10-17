// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How the container lies across its volumes. A spanned zip numbers its
// disks and gives every offset within one, so the container is the volumes
// end to end, and a volume's place in it is the length of every volume
// before it. A volume whose earlier lengths are not all known has no place
// yet.

use std::collections::BTreeMap;

use weaver_graph::{ByteRange, VolumeIndex};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Geometry {
    lengths: BTreeMap<u32, u64>,
}

impl Geometry {
    // Whether the length is news.
    pub(super) fn set(&mut self, volume: VolumeIndex, length: u64) -> bool {
        self.lengths.insert(volume.0, length).is_none()
    }

    pub(super) fn base(&self, volume: VolumeIndex) -> Option<u64> {
        (0..volume.0)
            .map(|earlier| self.lengths.get(&earlier))
            .sum()
    }

    // Where a disk's offset lies in the container.
    pub(super) fn global(&self, volume: VolumeIndex, offset: u64) -> Option<u64> {
        self.base(volume)?.checked_add(offset)
    }

    // The container's length, ending with the last volume.
    pub(super) fn total(&self, last: VolumeIndex) -> Option<u64> {
        self.base(VolumeIndex(last.0 + 1))
    }

    // A container range as volume ranges, in order, as far as the lengths
    // known reach.
    pub(super) fn to_volumes(&self, range: ByteRange, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        let mut base = 0u64;
        for volume in 0.. {
            let Some(length) = self.lengths.get(&volume).copied() else {
                return;
            };
            let end = base + length;
            if let Some(part) = range.intersect(ByteRange::new(base, end)) {
                out.push((VolumeIndex(volume), part.shift(-(base as i64))));
            }
            if end >= range.end {
                return;
            }
            base = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_volume_lies_after_every_volume_before_it() {
        let mut geometry = Geometry::default();
        assert!(geometry.set(VolumeIndex(0), 100));
        assert!(!geometry.set(VolumeIndex(0), 100));
        geometry.set(VolumeIndex(2), 50);
        assert_eq!(geometry.base(VolumeIndex(1)), Some(100));
        assert_eq!(geometry.base(VolumeIndex(2)), None);
        geometry.set(VolumeIndex(1), 70);
        assert_eq!(geometry.global(VolumeIndex(2), 5), Some(175));
        assert_eq!(geometry.total(VolumeIndex(2)), Some(220));
        let mut out = Vec::new();
        geometry.to_volumes(ByteRange::new(90, 180), &mut out);
        assert_eq!(
            out,
            vec![
                (VolumeIndex(0), ByteRange::new(90, 100)),
                (VolumeIndex(1), ByteRange::new(0, 70)),
                (VolumeIndex(2), ByteRange::new(0, 10)),
            ]
        );
    }
}
