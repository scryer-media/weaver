// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR2 object. It is built over the first .par2 member of a namespace
// and claims every other one as a further volume, so one object holds every
// recovery set in the namespace: a shared recovery file has to feed several
// sets, and a member is bound to one object only. Its volumes stay where
// the namespace put them, in the members' own files; the object reads its
// metadata as it passes and reads recovery payloads back when it spends
// them.

mod bind;
mod evidence;
mod repair;
mod sets;
mod walker;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use par2_rs::packet::{HEADER_SIZE, MAGIC};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Emit, Extent, Kind, MemberId, Namespace, Object, ObjectId, Placement,
    Provenance, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use self::bind::{Matched, bind_set};
use self::evidence::Evidence;
use self::repair::repair_set;
use self::sets::{Location, SetState, Volume};
use self::walker::Found;
use crate::grid::{Grid, WHOLE};

pub struct Par2 {
    id: ObjectId,
    volumes: Vec<Volume>,
    sets: BTreeMap<[u8; 16], SetState>,
    grid: Grid,
    // Each bound member's length, once all of it has arrived.
    lengths: BTreeMap<MemberId, u64>,
    // Damaged slices the last repair found while it waited for blocks.
    needed: u64,
    // Damaged slices the grid shows, recounted after the evidence changes.
    seen_damaged: Option<u64>,
    // What the last repair came to, for the members bound then.
    outcome: Option<RepairOutcome>,
}

impl Par2 {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: vec![Volume::default()],
            sets: BTreeMap::new(),
            grid: Grid::default(),
            lengths: BTreeMap::new(),
            needed: 0,
            seen_damaged: None,
            outcome: None,
        }
    }

    fn covered_members(&self) -> BTreeSet<MemberId> {
        self.sets
            .values()
            .flat_map(|state| state.members.values().copied())
            .collect()
    }

    // Set keys in the order their first packets were found.
    fn ordered(&self) -> Vec<[u8; 16]> {
        let mut keys: Vec<_> = self
            .sets
            .iter()
            .map(|(key, state)| (state.first_seen, *key))
            .collect();
        keys.sort();
        keys.into_iter().map(|(_, key)| key).collect()
    }

    fn volume(&mut self, index: VolumeIndex) -> &mut Volume {
        let at = index.0 as usize;
        if self.volumes.len() <= at {
            self.volumes.resize_with(at + 1, Volume::default);
        }
        &mut self.volumes[at]
    }

    // The smallest recovery packet a known set writes.
    fn smallest_packet(&self) -> Option<u64> {
        self.sets
            .values()
            .filter_map(|state| state.index.as_ref())
            .map(|index| index.slice_size + (HEADER_SIZE as u64) + 4)
            .min()
    }

    fn coming(&self) -> u64 {
        let packet = self.smallest_packet();
        self.volumes
            .iter()
            .map(|volume| volume.coming(packet))
            .sum()
    }

    fn present(&self, state: &SetState) -> u64 {
        state
            .recovery
            .values()
            .filter(|location| self.usable_location(location))
            .count() as u64
    }

    fn usable_location(&self, location: &Location) -> bool {
        self.volumes
            .get(location.volume.0 as usize)
            .is_some_and(|volume| !volume.is_spoiled(location.range))
    }

    fn block_sizes(&self) -> BTreeSet<u64> {
        self.sets
            .values()
            .filter_map(|state| state.index.as_ref())
            .map(|index| index.slice_size)
            .chain([WHOLE])
            .collect()
    }

    fn damaged_seen(&mut self) -> u64 {
        if let Some(count) = self.seen_damaged {
            return count;
        }
        let count = self
            .sets
            .values()
            .filter_map(|state| state.usable().map(|index| (state, index)))
            .flat_map(|(state, index)| {
                state
                    .members
                    .iter()
                    .map(move |(file, member)| (index, *file, *member))
            })
            .map(|(index, file, member)| {
                Evidence::of(
                    &self.grid,
                    index,
                    file,
                    member,
                    self.lengths.get(&member).copied(),
                )
                .vetoed()
                .filter(|verdict| *verdict == SliceVerdict::Damaged)
                .count() as u64
            })
            .sum();
        self.seen_damaged = Some(count);
        count
    }

    fn take(&mut self, volume: VolumeIndex, found: Vec<Found>) {
        for item in found {
            match item {
                Found::Metadata {
                    set,
                    at,
                    hash,
                    packet,
                } => {
                    self.sets
                        .entry(set)
                        .or_insert_with(|| SetState::new((volume, at)))
                        .take_metadata(hash, packet);
                    self.seen_damaged = None;
                }
                Found::Recovery {
                    set,
                    exponent,
                    range,
                } => {
                    self.sets
                        .entry(set)
                        .or_insert_with(|| SetState::new((volume, range.start)))
                        .recovery
                        .entry(exponent)
                        .or_insert(Location { volume, range });
                    self.volume(volume).walked_blocks += 1;
                }
            }
        }
    }
}

impl Object for Par2 {
    fn kind(&self) -> Kind {
        Kind::Par2
    }

    fn deliver(&mut self, delivery: Delivery<'_>, _out: &mut Vec<Emit>) {
        let mut found = Vec::new();
        let volume = self.volume(delivery.volume);
        volume.arrived.write(delivery.range(), delivery.provenance);
        volume.walker.deliver(
            delivery.offset,
            delivery.bytes,
            delivery.provenance == Provenance::Damaged,
            &mut found,
        );
        self.take(delivery.volume, found);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.volume(volume).closed = true;
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, _out: &mut Vec<Emit>) {
        self.volume(volume).lost.push(range);
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Moved and Unseekable both mean wait; only Lost changes what can be had.
    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if pull == Pull::Lost {
            self.volume_lost(seek.volume, seek.range, out);
        }
    }

    // The volume's bytes are all envelope: they stay in the volume's own
    // file, which is the .par2 member itself.
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        if first_bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
            return None;
        }
        self.volumes.push(Volume::named(name));
        Some(VolumeIndex((self.volumes.len() - 1) as u32))
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Par2 {
    fn bind(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>) {
        let before = self.covered_members();
        for key in self.ordered() {
            let Some(state) = self.sets.get_mut(&key) else {
                continue;
            };
            let Some(index) = state.usable() else {
                state.members.clear();
                continue;
            };
            let bound = bind_set(index, namespace, self.id);
            for (file, (member, matched)) in &bound {
                let described = index
                    .file_description(file)
                    .map(|description| description.filename.clone());
                let current = namespace.get(*member).map(|record| record.name.clone());
                if let (Matched::Content, Some(described)) = (matched, described)
                    && current.as_deref() != Some(described.as_str())
                    && namespace.rename(*member, &described).is_some()
                {
                    renamed.push(*member);
                }
            }
            state.members = bound
                .into_iter()
                .map(|(file, (member, _))| (file, member))
                .collect();
        }
        self.lengths = self
            .sets
            .values()
            .flat_map(|state| state.members.values())
            .filter_map(|member| {
                let record = namespace.get(*member)?;
                record
                    .length
                    .filter(|_| record.is_complete())
                    .map(|length| (*member, length))
            })
            .collect();
        self.seen_damaged = None;
        if self.covered_members() != before {
            self.outcome = None;
        }
    }

    fn covers(&self, member: MemberId) -> bool {
        self.sets
            .values()
            .any(|state| state.bound(member).is_some())
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        for block in self.block_sizes() {
            for segment in segments {
                self.grid.offer(member, block, segment);
            }
        }
        self.seen_damaged = None;
    }

    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        let found = self.ordered().into_iter().find_map(|key| {
            let state = self.sets.get(&key)?;
            Some((state.usable()?, state.bound(member)?))
        });
        if let Some((index, file)) = found {
            let length = self.lengths.get(&member).copied();
            out.extend(Evidence::of(&self.grid, index, file, member, length).vetoed());
        }
    }

    // Ask for the volumes not yet in hand, in order, until what they are
    // named to carry covers the shortfall.
    fn wants(&mut self, out: &mut Vec<Seek>) {
        let wanted = self.needed.max(self.damaged_seen());
        let present: u64 = self.sets.values().map(|state| self.present(state)).sum();
        let mut shortfall = wanted.saturating_sub(present);
        let packet = self.smallest_packet();
        for (index, volume) in self.volumes.iter_mut().enumerate() {
            if shortfall == 0 {
                break;
            }
            if volume.closed {
                continue;
            }
            shortfall = shortfall.saturating_sub(volume.coming(packet));
            if volume.asked {
                continue;
            }
            volume.asked = true;
            let mut cursor = 0;
            for run in volume.arrived.runs() {
                if run.range.start > cursor {
                    out.push(Seek::recovery(
                        VolumeIndex(index as u32),
                        ByteRange::new(cursor, run.range.start),
                        Weight::FIRST,
                    ));
                }
                cursor = run.range.end;
            }
            // The volume's length is the parent's to know; the root clamps
            // an open end to it.
            out.push(Seek::recovery(
                VolumeIndex(index as u32),
                ByteRange::new(cursor, u64::MAX),
                Weight::FIRST,
            ));
        }
    }

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let coming = self.coming();
        let mut settled = BTreeSet::new();
        let mut outcome = RepairOutcome::Clean;
        let mut waiting = 0;
        for key in self.ordered() {
            let Some(state) = self.sets.get_mut(&key) else {
                continue;
            };
            let result = repair_set(state, &self.grid, &self.volumes, coming, &settled, access);
            if result.outcome == RepairOutcome::Waiting {
                waiting += result.damaged;
            }
            settled.extend(result.repaired);
            outcome = combine(outcome, result.outcome);
        }
        self.needed = waiting;
        self.outcome = Some(outcome);
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.outcome
    }

    fn blocks_reachable(&self) -> u64 {
        let present: u64 = self.sets.values().map(|state| self.present(state)).sum();
        present + self.coming()
    }
}

// Several sets' outcomes as one: the worst kind wins, and outcomes of one
// kind add up.
pub(crate) fn combine(left: RepairOutcome, right: RepairOutcome) -> RepairOutcome {
    fn rank(outcome: RepairOutcome) -> u8 {
        match outcome {
            RepairOutcome::Clean => 0,
            RepairOutcome::Repaired { .. } => 1,
            RepairOutcome::Waiting => 2,
            RepairOutcome::Impossible { .. } => 3,
        }
    }
    match (left, right) {
        (RepairOutcome::Repaired { slices: a }, RepairOutcome::Repaired { slices: b }) => {
            RepairOutcome::Repaired { slices: a + b }
        }
        (
            RepairOutcome::Impossible {
                damaged: a,
                reachable: x,
            },
            RepairOutcome::Impossible {
                damaged: b,
                reachable: y,
            },
        ) => RepairOutcome::Impossible {
            damaged: a + b,
            reachable: x + y,
        },
        _ if rank(right) > rank(left) => right,
        _ => left,
    }
}
