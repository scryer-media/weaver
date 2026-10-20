// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How the container lies across the volumes. A split set is a pure byte
// split: every volume but the last is as long as the first, and the last
// holds the rest, plus whatever was appended after the container's end.

use weaver_graph::{ByteRange, VolumeIndex};

pub(super) const MAX_PARTS: u64 = 100_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct Geometry {
    part: u64,
    total: u64,
    parts: u32,
}

impl Geometry {
    // From the first volume's length and the container's, which the
    // signature header gives. `None` for a shape no set has.
    pub(super) fn new(first: u64, total: u64) -> Option<Self> {
        if first == 0 {
            return None;
        }
        let parts = if first >= total {
            1
        } else {
            total.div_ceil(first)
        };
        if parts > MAX_PARTS {
            return None;
        }
        Some(Self {
            part: first,
            total,
            parts: parts as u32,
        })
    }

    pub(super) fn last(&self) -> VolumeIndex {
        VolumeIndex(self.parts - 1)
    }

    pub(super) fn base(&self, volume: VolumeIndex) -> u64 {
        u64::from(volume.0) * self.part
    }

    // How much of the container a volume holds: as much as the first for
    // every volume but the last, the rest of the container for the last.
    pub(super) fn holds(&self, volume: VolumeIndex) -> u64 {
        if volume == self.last() {
            return self.total - self.base(volume);
        }
        self.part
    }

    // The length a volume must have: exact for every volume but the last,
    // a floor for the last, which may carry bytes after the container.
    pub(super) fn agrees(&self, volume: VolumeIndex, length: u64) -> bool {
        volume.0 < self.parts
            && (length == self.holds(volume)
                || volume == self.last() && length > self.holds(volume))
    }

    // A volume range in container coordinates, clipped to what the geometry
    // gives that volume. `None` for a volume beyond the set.
    pub(super) fn to_container(&self, volume: VolumeIndex, range: ByteRange) -> Option<ByteRange> {
        if volume.0 >= self.parts {
            return None;
        }
        let base = self.base(volume);
        let end = if volume == self.last() {
            range.end
        } else {
            range.end.min(self.part)
        };
        let start = range.start.min(end);
        Some(ByteRange::new(base + start, base + end))
    }

    // A container range as volume ranges, in order.
    pub(super) fn to_volumes(&self, range: ByteRange, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        let mut cursor = range.start;
        while cursor < range.end {
            let index = (cursor / self.part).min(u64::from(self.parts - 1)) as u32;
            let volume = VolumeIndex(index);
            let base = self.base(volume);
            let end = if volume == self.last() {
                range.end
            } else {
                range.end.min(base + self.part)
            };
            out.push((volume, ByteRange::new(cursor - base, end - base)));
            cursor = end;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_split_set_is_a_pure_byte_split() {
        let geometry = Geometry::new(100, 250).unwrap();
        assert_eq!(geometry.last(), VolumeIndex(2));
        let mut out = Vec::new();
        geometry.to_volumes(ByteRange::new(90, 260), &mut out);
        assert_eq!(
            out,
            vec![
                (VolumeIndex(0), ByteRange::new(90, 100)),
                (VolumeIndex(1), ByteRange::new(0, 100)),
                (VolumeIndex(2), ByteRange::new(0, 60)),
            ]
        );
        assert_eq!(
            geometry.to_container(VolumeIndex(1), ByteRange::new(10, 120)),
            Some(ByteRange::new(110, 200))
        );
        assert!(geometry.agrees(VolumeIndex(2), 50));
        assert!(!geometry.agrees(VolumeIndex(2), 49));
        assert!(!geometry.agrees(VolumeIndex(1), 99));
    }

    #[test]
    fn one_volume_holds_the_container_and_what_follows_it() {
        let geometry = Geometry::new(300, 250).unwrap();
        assert_eq!(geometry.last(), VolumeIndex(0));
        assert_eq!(
            geometry.to_container(VolumeIndex(0), ByteRange::new(240, 300)),
            Some(ByteRange::new(240, 300))
        );
        assert_eq!(
            geometry.to_container(VolumeIndex(1), ByteRange::new(0, 1)),
            None
        );
    }
}
