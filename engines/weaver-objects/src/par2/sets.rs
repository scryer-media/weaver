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
use crate::recovery_volume;

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
    // Which member each described file is bound to: by its front, its
    // name, or the slices a repair read back.
    members: BTreeMap<FileId, MemberId>,
    // The complete members nothing named that could be a file of the set
    // no member is bound to: what a repair reads back for its slices, or
    // binds by elimination.
    unnamed: Vec<(MemberId, Length)>,
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
            unnamed: Vec::new(),
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
        let described = self.usable().is_some();
        self.packets.change(|packets| packets.0.push(packet));
        self.index = index_of(&self.packets.get().0, output_limit);
        self.whole = whole_of(self.index.as_ref());
        // A ruling made with no description was on nothing: the set rules
        // again on what it now describes.
        if !described && self.usable().is_some() {
            self.last = None;
        }
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

    // An index that describes every file it protects: each has its
    // description, which names it, sizes it and hashes it whole. Its slice
    // checksums, when the set carries them, say which slices are good; a
    // file without them is checked whole and rebuilt whole.
    pub(super) fn usable(&self) -> Option<&Par2FileSet> {
        let index = self.index.as_ref()?;
        index
            .recovery_file_ids
            .iter()
            .all(|id| index.file_description(id).is_some())
            .then_some(index)
    }

    // The CRC32 the described file's slices fold to.
    pub(super) fn whole(&self, file: FileId) -> Option<u32> {
        self.whole.get(&file).copied()
    }

    pub(super) fn members(&self) -> &BTreeMap<FileId, MemberId> {
        &self.members
    }

    // A verdict was on the members bound then and the unnamed ones it read;
    // others, or the same members bound to other files, void it. Returns
    // whether it was voided.
    pub(super) fn bind(
        &mut self,
        mut members: BTreeMap<FileId, MemberId>,
        unnamed: &[(MemberId, Length)],
    ) -> bool {
        // A member bound before that nothing names now, as one a repair
        // knew by its slices, stays that file while no other member is.
        for (file, member) in &self.members {
            if unnamed.iter().any(|(candidate, _)| candidate == member) {
                members.entry(*file).or_insert(*member);
            }
        }
        let free: Vec<u64> = self.usable().map_or_else(Vec::new, |index| {
            index
                .recovery_file_ids
                .iter()
                .filter(|file| !members.contains_key(file))
                .filter_map(|file| Some(index.file_description(file)?.length))
                .collect()
        });
        let unnamed: Vec<_> = unnamed
            .iter()
            .copied()
            .filter(|(member, length)| {
                free.iter().any(|long| length.fits(*long))
                    && !members.values().any(|bound| bound == member)
            })
            .collect();
        let changed = members != self.members || unnamed != self.unnamed;
        if changed {
            self.members = members;
            self.unnamed = unnamed;
            self.last = None;
        }
        changed
    }

    pub(super) fn unnamed(&self) -> &[(MemberId, Length)] {
        &self.unnamed
    }

    // An unnamed member that could be a described file may be it until a
    // repair has read it.
    pub(super) fn may_be(&self, length: u64) -> bool {
        self.last.is_none() && self.unnamed.iter().any(|(_, long)| long.fits(length))
    }

    // The repair knew the unnamed member as the file, by a slice it read
    // back or by elimination: it is it.
    pub(super) fn know(&mut self, file: FileId, member: MemberId) {
        self.unnamed.retain(|(candidate, _)| *candidate != member);
        self.members.insert(file, member);
    }

    // Damage shown since an unread verdict may be what the descriptions
    // the set has not read cover, so the set is tried again. An impossible
    // verdict stands: damage only grows, so what the set could not rebuild
    // it still cannot.
    pub(super) fn damage_shown(&mut self) {
        if self
            .last
            .is_some_and(|last| last.outcome == RepairOutcome::Unread)
        {
            self.last = None;
        }
    }

    // An impossible verdict counted the blocks of the volumes bound then,
    // and an unread one the descriptions they could bring; a volume bound
    // since may bring more of either, so the set is tried again. Without
    // this the verdict would depend on whether a volume's front decided
    // before or after the damage arrived.
    pub(super) fn reopen(&mut self) {
        let stale = matches!(
            self.last.map(|last| last.outcome),
            Some(RepairOutcome::Impossible { .. } | RepairOutcome::Unread)
        );
        if stale {
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
    // The name it was bound under, which a later volume of the set continues.
    pub(super) name: Option<String>,
}

impl Volume {
    pub(super) fn name(&mut self, name: &str) {
        self.named_blocks = recovery_volume::named_blocks(name, ".par2");
        self.name = Some(name.to_owned());
    }

    // Whether the volume can bring the set blocks: it carries the set's
    // packets, or nothing of it has been walked to say whose it is.
    pub(super) fn serves(&self, set: &[u8; 16]) -> bool {
        self.sets.is_empty() || self.sets.contains(set)
    }

    // Whether the walk has shown the volume carries recovery blocks: one
    // passed, the packet it is reading is one, or its name says it carries
    // some and its packets say whose.
    pub(super) fn shows_recovery(&self) -> bool {
        self.walked_blocks > 0
            || self.walker.reading_recovery()
            || (self.named_blocks.unwrap_or(0) > 0 && !self.sets.is_empty())
    }

    // Ask for every byte of the volume not yet in hand.
    pub(super) fn ask(&self, index: VolumeIndex, out: &mut Vec<Seek>) {
        recovery_volume::gaps(&self.arrived, |range| {
            out.push(Seek::recovery(index, range, Weight::FIRST))
        });
    }

    // Say every byte of the volume not yet in hand is not wanted yet.
    pub(super) fn defer(&self, index: VolumeIndex, out: &mut Vec<Seek>) {
        recovery_volume::gaps(&self.arrived, |range| {
            out.push(Seek::deferred(index, range))
        });
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

// What is known of an unnamed member's length. A member some byte of which
// decoded knows its length exactly; one none of whose did is only bounded
// by its post's encoded size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Length {
    Exact(u64),
    AtMost(u64),
}

impl Length {
    // Whether the member could be a described file of `long`.
    pub(super) fn fits(self, long: u64) -> bool {
        match self {
            Self::Exact(length) => length == long,
            Self::AtMost(bound) => long <= bound,
        }
    }

    pub(super) fn bound(self) -> u64 {
        match self {
            Self::Exact(length) | Self::AtMost(length) => length,
        }
    }
}
