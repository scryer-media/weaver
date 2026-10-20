// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the object knows of each recovery set, and of each of its volumes.
//
// A set is identified by its recovery set id, the id in every packet's
// header, so a packet belongs to the set it names wherever it was found: a
// shared recovery file feeds every set whose packets it carries.

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::{FileId, Packet, Par2FileSet};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Coverage, MemberId, Sealed, Seek, VolumeIndex, Weight};

use super::facts::Packets;
use super::repair::SetRepair;
use super::walker::Walker;

const MAX_PAR2_INPUT_SLICES: u64 = 32_768;

// Where one recovery packet sits, header included.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Location {
    pub(super) volume: VolumeIndex,
    pub(super) range: ByteRange,
}

#[derive(Debug)]
pub(super) struct SetState {
    // Where the set's first packet was found: sets are taken in this order,
    // which is the order of the volumes that carry them.
    pub(super) first_seen: (VolumeIndex, u64),
    // Sealed: the metadata settles in the first volumes, and every
    // checkpoint after that carries it as it was encoded then.
    packets: Sealed<Packets>,
    hashes: BTreeSet<[u8; 16]>,
    // The set as far as its metadata has arrived; `None` until its main
    // packet has.
    pub(super) index: Option<Par2FileSet>,
    // Each described file's whole CRC32, folded from its slice checksums
    // when the index is built: the index is fixed until a new packet
    // rebuilds it, so the fold is too.
    whole: BTreeMap<FileId, u32>,
    // Recovery packets by exponent: the first copy found is the one kept.
    pub(super) recovery: BTreeMap<u32, Location>,
    // Which member each described file is bound to.
    members: BTreeMap<FileId, MemberId>,
    // What the set's last repair came to, over the members bound then.
    pub(super) last: Option<SetRepair>,
}

impl SetState {
    pub(super) fn new(first_seen: (VolumeIndex, u64)) -> Self {
        Self {
            first_seen,
            packets: Sealed::default(),
            hashes: BTreeSet::new(),
            index: None,
            whole: BTreeMap::new(),
            recovery: BTreeMap::new(),
            members: BTreeMap::new(),
            last: None,
        }
    }

    // Take a metadata packet; the index is rebuilt when it is new. A set's
    // metadata is a few packets per file, so rebuilding is cheaper than
    // keeping two ways of growing it. A set whose geometry is past the
    // limits has no index.
    pub(super) fn take_metadata(&mut self, hash: [u8; 16], packet: Packet, output_limit: u64) {
        if !self.hashes.insert(hash) {
            return;
        }
        self.packets.change(|packets| packets.0.push(packet));
        self.index = index_of(&self.packets.get().0, output_limit);
        self.whole = whole_of(self.index.as_ref());
    }

    // A set as a checkpoint recorded it: the index is rebuilt from its
    // packets, as each new one rebuilt it.
    pub(super) fn restored(
        first_seen: (VolumeIndex, u64),
        packets: Sealed<Packets>,
        hashes: BTreeSet<[u8; 16]>,
        output_limit: u64,
    ) -> Self {
        let index = index_of(&packets.get().0, output_limit);
        Self {
            whole: whole_of(index.as_ref()),
            index,
            packets,
            hashes,
            ..Self::new(first_seen)
        }
    }

    pub(super) fn packets(&self) -> &Sealed<Packets> {
        &self.packets
    }

    pub(super) fn hashes(&self) -> &BTreeSet<[u8; 16]> {
        &self.hashes
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

    // The CRC32 the described file's slices fold to.
    pub(super) fn whole(&self, file: FileId) -> Option<u32> {
        self.whole.get(&file).copied()
    }

    pub(super) fn members(&self) -> &BTreeMap<FileId, MemberId> {
        &self.members
    }

    // A verdict was on the members bound then; other members, or the same
    // members bound to other files, void it. Returns whether it was voided.
    pub(super) fn bind(&mut self, members: BTreeMap<FileId, MemberId>) -> bool {
        let changed = members != self.members;
        if changed {
            self.members = members;
            self.last = None;
        }
        changed
    }

    // An impossible verdict counted the blocks of the volumes bound then; a
    // volume bound since may bring more, so the set is tried again. Without
    // this the verdict would depend on whether a volume's front decided
    // before or after the damage arrived.
    pub(super) fn reopen(&mut self) {
        let impossible = matches!(
            self.last.map(|last| last.outcome),
            Some(RepairOutcome::Impossible { .. })
        );
        if impossible {
            self.last = None;
        }
    }

    // The recovery blocks the set must hold before it is tried again, when
    // its last repair came up short of them.
    pub(super) fn short_of(&self) -> Option<u64> {
        self.last
            .filter(|last| last.outcome == RepairOutcome::Waiting)
            .map(|last| last.needed)
    }

    // Whether the last repair settled the set: only a wait is tried again.
    pub(super) fn concluded(&self) -> Option<RepairOutcome> {
        self.last
            .map(|last| last.outcome)
            .filter(|outcome| *outcome != RepairOutcome::Waiting)
    }

    pub(super) fn bound(&self, member: MemberId) -> Option<FileId> {
        self.members
            .iter()
            .find(|(_, bound)| **bound == member)
            .map(|(file, _)| *file)
    }
}

#[derive(Default)]
pub(super) struct Volume {
    pub(super) walker: Walker,
    pub(super) arrived: Coverage,
    pub(super) lost: Vec<ByteRange>,
    pub(super) closed: bool,
    // The recovery blocks the volume's name says it carries.
    pub(super) named_blocks: Option<u64>,
    pub(super) walked_blocks: u64,
    // The sets whose packets the walk has found in it.
    pub(super) sets: BTreeSet<[u8; 16]>,
}

impl Volume {
    pub(super) fn name(&mut self, name: &str) {
        self.named_blocks = named_blocks(name);
    }

    // Whether the volume can bring the set blocks: it carries the set's
    // packets, or nothing of it has been walked to say whose it is.
    pub(super) fn serves(&self, set: &[u8; 16]) -> bool {
        self.sets.is_empty() || self.sets.contains(set)
    }

    // Ask for every byte of the volume not yet in hand.
    pub(super) fn ask(&self, index: VolumeIndex, out: &mut Vec<Seek>) {
        let mut cursor = 0;
        for run in self.arrived.runs() {
            if run.range.start > cursor {
                out.push(Seek::recovery(
                    index,
                    ByteRange::new(cursor, run.range.start),
                    Weight::FIRST,
                ));
            }
            cursor = run.range.end;
        }
        // The volume's length is the parent's to know; the root clamps an
        // open end to it.
        out.push(Seek::recovery(
            index,
            ByteRange::new(cursor, u64::MAX),
            Weight::FIRST,
        ));
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

fn index_of(packets: &[Packet], output_limit: u64) -> Option<Par2FileSet> {
    Par2FileSet::from_packets(packets.to_vec())
        .ok()
        .filter(|set| validate_par2_geometry(set, output_limit).is_ok())
}

fn whole_of(index: Option<&Par2FileSet>) -> BTreeMap<FileId, u32> {
    let Some(index) = index else {
        return BTreeMap::new();
    };
    index
        .files
        .keys()
        .filter_map(|file| Some((*file, index.expected_file_crc32(*file)?)))
        .collect()
}

// Admit declared geometry before anything is built on it.
fn validate_par2_geometry(set: &Par2FileSet, output_limit: u64) -> par2_rs::Result<()> {
    if set.slice_size == 0 {
        return Err(par2_resource_error("zero slice size"));
    }
    if set.slice_size > output_limit {
        return Err(par2_resource_error("slice size exceeds the output budget"));
    }
    let mut described_bytes = 0u64;
    let mut tracked_slices = 0u64;
    for description in set.files.values() {
        described_bytes = described_bytes
            .checked_add(description.length)
            .filter(|bytes| *bytes <= output_limit)
            .ok_or_else(|| par2_resource_error("described output exceeds the admitted job size"))?;
        // Non-recovery descriptions also reach verification bitmaps. Bound
        // those allocations independently of PAR2's repair-input cap.
        tracked_slices = tracked_slices
            .checked_add(description.length.div_ceil(set.slice_size))
            .filter(|slices| *slices <= 1_048_576)
            .ok_or_else(|| {
                par2_resource_error("verification bitmap exceeds the metadata budget")
            })?;
    }
    let mut input_slices = 0u64;
    for description in set
        .recovery_file_ids
        .iter()
        .filter_map(|id| set.files.get(id))
    {
        input_slices = input_slices
            .checked_add(description.length.div_ceil(set.slice_size))
            .filter(|slices| *slices <= MAX_PAR2_INPUT_SLICES)
            .ok_or_else(|| par2_resource_error("input slice count exceeds the repair limit"))?;
    }
    Ok(())
}

fn par2_resource_error(reason: &str) -> par2_rs::Par2Error {
    par2_rs::Par2Error::ResourceLimitExceeded {
        reason: format!("WEAVER_RESOURCE_LIMIT[par2_metadata]: {reason}"),
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
