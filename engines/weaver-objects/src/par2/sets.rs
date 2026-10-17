// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the object knows of each recovery set, and of each of its volumes.
//
// A set is identified by its recovery set id, the id in every packet's
// header, so a packet belongs to the set it names wherever it was found: a
// shared recovery file feeds every set whose packets it carries.

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::{FileId, Packet, Par2FileSet};
use weaver_graph::{ByteRange, Coverage, MemberId, VolumeIndex};

use super::walker::Walker;

// Where one recovery packet sits, header included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Location {
    pub(super) volume: VolumeIndex,
    pub(super) range: ByteRange,
}

#[derive(Debug)]
pub(super) struct SetState {
    // Where the set's first packet was found: sets are taken in this order,
    // which is the order of the volumes that carry them.
    pub(super) first_seen: (VolumeIndex, u64),
    packets: Vec<Packet>,
    hashes: BTreeSet<[u8; 16]>,
    // The set as far as its metadata has arrived; `None` until its main
    // packet has.
    pub(super) index: Option<Par2FileSet>,
    // Recovery packets by exponent: the first copy found is the one kept.
    pub(super) recovery: BTreeMap<u32, Location>,
    // Which member each described file is bound to.
    pub(super) members: BTreeMap<FileId, MemberId>,
}

impl SetState {
    pub(super) fn new(first_seen: (VolumeIndex, u64)) -> Self {
        Self {
            first_seen,
            packets: Vec::new(),
            hashes: BTreeSet::new(),
            index: None,
            recovery: BTreeMap::new(),
            members: BTreeMap::new(),
        }
    }

    // Take a metadata packet; the index is rebuilt when it is new. A set's
    // metadata is a few packets per file, so rebuilding is cheaper than
    // keeping two ways of growing it.
    pub(super) fn take_metadata(&mut self, hash: [u8; 16], packet: Packet) {
        if !self.hashes.insert(hash) {
            return;
        }
        self.packets.push(packet);
        self.index = Par2FileSet::from_packets(self.packets.clone()).ok();
    }

    // An index that can verify every file it protects: each has its
    // description and, unless empty, its slice checksums.
    pub(super) fn usable(&self) -> Option<&Par2FileSet> {
        let index = self.index.as_ref()?;
        index
            .recovery_file_ids
            .iter()
            .all(|id| {
                index.file_description(id).is_some_and(|description| {
                    description.length == 0 || index.file_checksums(id).is_some()
                })
            })
            .then_some(index)
    }

    pub(super) fn bound(&self, member: MemberId) -> Option<FileId> {
        self.members
            .iter()
            .find(|(_, bound)| **bound == member)
            .map(|(file, _)| *file)
    }
}

#[derive(Debug, Default)]
pub(super) struct Volume {
    pub(super) walker: Walker,
    pub(super) arrived: Coverage,
    pub(super) lost: Vec<ByteRange>,
    pub(super) closed: bool,
    // The recovery blocks the volume's name says it carries.
    pub(super) named_blocks: Option<u64>,
    pub(super) walked_blocks: u64,
    // A recovery seek for it is outstanding.
    pub(super) asked: bool,
}

impl Volume {
    pub(super) fn named(name: &str) -> Self {
        Self {
            named_blocks: named_blocks(name),
            ..Self::default()
        }
    }

    pub(super) fn is_spoiled(&self, range: ByteRange) -> bool {
        self.walker.is_damaged(range)
            || self.lost.iter().any(|lost| lost.intersect(range).is_some())
    }

    // Recovery blocks still to come, by the volume's name, less what was
    // walked and what a lost range could have held. `packet` is the smallest
    // recovery packet any known set writes, so a loss is never undercounted.
    pub(super) fn coming(&self, packet: Option<u64>) -> u64 {
        if self.closed {
            return 0;
        }
        let named = self.named_blocks.unwrap_or(0);
        let lost: u64 = match packet {
            Some(packet) if packet > 0 => self
                .lost
                .iter()
                .map(|range| range.len().div_ceil(packet) + 1)
                .sum(),
            _ if self.lost.is_empty() => 0,
            _ => named,
        };
        named
            .saturating_sub(self.walked_blocks)
            .saturating_sub(lost)
    }
}

// `name.vol07+08.par2` carries eight recovery blocks, by the convention
// every PAR2 creator follows.
fn named_blocks(name: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let stem = lower.strip_suffix(".par2")?;
    let (_, tail) = stem.rsplit_once(".vol")?;
    let (_, count) = tail.split_once('+')?;
    count.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::named_blocks;

    #[test]
    fn a_volume_name_says_how_many_blocks_it_carries() {
        assert_eq!(named_blocks("set.vol07+08.par2"), Some(8));
        assert_eq!(named_blocks("SET.VOL000+128.PAR2"), Some(128));
        assert_eq!(named_blocks("set.par2"), None);
        assert_eq!(named_blocks("set.vol1.par2"), None);
    }
}
