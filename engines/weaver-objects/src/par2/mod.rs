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
pub(crate) mod facts;
mod repair;
mod sets;
mod walker;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use par2_rs::Par2FileSet;
use par2_rs::packet::{HEADER_SIZE, MAGIC};
use weaver_graph::recovery::{MemberAccess, RepairOutcome, SetRuling};
use weaver_graph::{
    ByteRange, Delivery, Emit, Extent, Kind, MemberId, Namespace, Object, ObjectId, Placement,
    Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex,
};

use self::bind::bind_sets;
use self::evidence::Evidence;
use self::repair::repair_set;
use self::sets::{Location, SetState, Volume};
use self::walker::{Found, Meter};
use crate::grid::{Grid, WHOLE};

// One job holds no more recovery sets than an NZB holds files.
const MAX_PAR2_RECOVERY_SETS: usize = 100_000;
// What a job may describe, and what one repair may hold in memory.
const MAX_JOB_BYTES: u64 = 2 * 1024 * 1024 * 1024 * 1024;
const REPAIR_MEMORY_LIMIT: usize = 128 * 1024 * 1024;

pub struct Par2 {
    id: ObjectId,
    volumes: Vec<Volume>,
    sets: BTreeMap<[u8; 16], SetState>,
    grid: Grid,
    // Each bound member's length, once all of it has arrived.
    lengths: BTreeMap<MemberId, u64>,
    // What the grid says of each bound member, per set: folded where its
    // inputs change (a member's slices in `feed`, a set's index in `take`,
    // its bindings and lengths in `bind`) and read everywhere else.
    evidence: BTreeMap<([u8; 16], MemberId), Evidence>,
    // What the last repair came to, for the members bound then.
    outcome: Option<RepairOutcome>,
    meter: Meter,
    // A job past the packet or set limits keeps none of its sets.
    refused: bool,
    output_limit: u64,
    memory_limit: usize,
}

impl Par2 {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: Vec::new(),
            sets: BTreeMap::new(),
            grid: Grid::default(),
            lengths: BTreeMap::new(),
            evidence: BTreeMap::new(),
            outcome: None,
            meter: Meter::default(),
            refused: false,
            output_limit: MAX_JOB_BYTES,
            memory_limit: REPAIR_MEMORY_LIMIT,
        }
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

    // Recovery blocks still to come from the volumes that serve the set.
    fn coming(&self, set: &[u8; 16]) -> u64 {
        let packet = self.smallest_packet();
        self.volumes
            .iter()
            .filter(|volume| volume.serves(set))
            .map(|volume| volume.coming(packet))
            .sum()
    }

    // Every grid the evidence is folded on: each known set's slices, and
    // the whole member.
    fn grids(&self) -> BTreeSet<u64> {
        self.slice_sizes().chain([WHOLE]).collect()
    }

    fn slice_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        self.sets
            .values()
            .filter_map(|state| state.index.as_ref())
            .map(|index| index.slice_size)
    }

    // Damaged slices the evidence shows, per set.
    fn damaged_seen(&self) -> BTreeMap<[u8; 16], u64> {
        let mut seen = BTreeMap::new();
        for ((key, _), evidence) in &self.evidence {
            *seen.entry(*key).or_default() += evidence
                .vetoed()
                .filter(|verdict| *verdict == SliceVerdict::Damaged)
                .count() as u64;
        }
        seen
    }

    // Fold every bound member of the set afresh: its index, its bindings or
    // its members' lengths changed, and each verdict reads all three.
    pub(super) fn fold_set(&mut self, key: [u8; 16]) {
        self.evidence.retain(|(set, _), _| *set != key);
        let Some(state) = self.sets.get(&key) else {
            return;
        };
        let Some(index) = state.usable() else {
            return;
        };
        for (file, member) in state.members() {
            self.evidence.entry((key, *member)).or_insert_with(|| {
                Evidence::of(
                    &self.grid,
                    index,
                    state.whole(*file),
                    *file,
                    *member,
                    self.lengths.get(member).copied(),
                )
            });
        }
    }

    // A set that waited on recovery blocks waits no longer once the blocks
    // in hand cover the slices it found damaged, or once its volumes can
    // bring no more: the wait's outcome is taken back, so the set rules in
    // this step and repairs or fails.
    fn end_wait(&mut self) {
        let over = self.sets.iter().any(|(key, state)| {
            let needed = state.needed();
            needed > 0 && (state.recovery.len() as u64 >= needed || self.coming(key) == 0)
        });
        if over {
            self.outcome = None;
        }
    }

    fn take(&mut self, volume: VolumeIndex, found: Vec<Found>) {
        for item in found {
            if self.refused {
                return;
            }
            let set = match &item {
                Found::Metadata { set, .. } | Found::Recovery { set, .. } => Some(*set),
                Found::Unused => None,
            };
            let new_set = set.is_some_and(|set| !self.sets.contains_key(&set));
            if !self.meter.charge(&item) || (new_set && self.sets.len() >= MAX_PAR2_RECOVERY_SETS) {
                self.refused = true;
                self.sets.clear();
                self.evidence.clear();
                return;
            }
            match item {
                Found::Unused => {}
                Found::Metadata {
                    set,
                    at,
                    hash,
                    packet,
                } => {
                    self.sets
                        .entry(set)
                        .or_insert_with(|| SetState::new((volume, at)))
                        .take_metadata(hash, packet, self.output_limit);
                    self.volume(volume).sets.insert(set);
                    self.fold_set(set);
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
                    let carrier = self.volume(volume);
                    carrier.walked_blocks += 1;
                    carrier.sets.insert(set);
                }
            }
        }
    }
}

impl Object for Par2 {
    fn kind(&self) -> Kind {
        Kind::Par2
    }

    fn facts(&self) -> weaver_graph::Facts {
        facts::of(self)
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
        let mut found = Vec::new();
        let volume = self.volume(delivery.volume);
        volume.arrived.write(delivery.range(), delivery.provenance);
        volume
            .walker
            .deliver(delivery.offset, delivery.bytes, &mut found);
        self.take(delivery.volume, found);
        self.end_wait();
    }

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.volume(volume).closed = true;
        self.end_wait();
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, _out: &mut Vec<Emit>) {
        let mut found = Vec::new();
        let state = self.volume(volume);
        state.lost.push(range);
        state.walker.lose(range, &mut found);
        // A recovery packet whose bytes are lost cannot be read back.
        for state in self.sets.values_mut() {
            state
                .recovery
                .retain(|_, at| at.volume != volume || at.range.intersect(range).is_none());
        }
        self.take(volume, found);
        self.end_wait();
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Every answer means wait. A seek asks for the rest of a volume, and one
    // lost article loses it whole, though the blocks beyond that article
    // still come; what is lost reaches `volume_lost` at its exact range.
    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

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

    // Every PAR2 file is a volume of the one object, numbered as it comes.
    fn claim(&mut self, _name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        (first_bytes.get(..MAGIC.len()) == Some(MAGIC.as_slice()))
            .then(|| VolumeIndex(self.volumes.len() as u32))
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8]) {
        self.volume(volume).name(name);
        let mut reopened = false;
        for state in self.sets.values_mut() {
            reopened |= state.reopen();
        }
        // The object's verdict is its sets' verdicts: one tried again leaves
        // the object unruled until it repairs again.
        if reopened {
            self.outcome = None;
        }
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
        let (keys, indexes): (Vec<[u8; 16]>, Vec<&Par2FileSet>) = self
            .ordered()
            .into_iter()
            .filter_map(|key| Some((key, self.sets[&key].usable()?)))
            .unzip();
        let (bound, names) = bind_sets(&indexes, namespace, self.id);
        let mut bound: BTreeMap<_, _> = keys.into_iter().zip(bound).collect();
        for (member, described) in names {
            let current = namespace.get(member).map(|record| record.name.as_str());
            if current != Some(described.as_str()) && namespace.rename(member, &described).is_some()
            {
                renamed.push(member);
            }
        }
        let mut moved = BTreeSet::new();
        for (key, state) in &mut self.sets {
            if state.bind(bound.remove(key).unwrap_or_default()) {
                moved.insert(*key);
            }
        }
        let voided = !moved.is_empty();
        let lengths: BTreeMap<MemberId, u64> = self
            .sets
            .values()
            .flat_map(|state| state.members().values())
            .filter_map(|member| {
                let record = namespace.get(*member)?;
                record
                    .length
                    .filter(|_| record.is_complete())
                    .map(|length| (*member, length))
            })
            .collect();
        for (key, state) in &self.sets {
            let lengthened = state
                .members()
                .values()
                .any(|member| lengths.get(member) != self.lengths.get(member));
            if lengthened {
                moved.insert(*key);
            }
        }
        self.lengths = lengths;
        for key in moved {
            self.fold_set(key);
        }
        // The object's verdict is its sets' verdicts.
        if voided {
            self.outcome = None;
        }
    }

    // Every file a usable set can rebuild that no member is bound to, once
    // each, in set order.
    fn absent(&self, out: &mut Vec<(String, u64)>) {
        for key in self.ordered() {
            let state = &self.sets[&key];
            let Some(index) = state.usable() else {
                continue;
            };
            for file in &index.recovery_file_ids {
                let Some(description) = index.file_description(file) else {
                    continue;
                };
                let named = (description.filename.clone(), description.length);
                if !state.members().contains_key(file) && !out.contains(&named) {
                    out.push(named);
                }
            }
        }
    }

    fn covers(&self, member: MemberId) -> bool {
        self.sets
            .values()
            .any(|state| state.bound(member).is_some())
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        for block in self.grids() {
            for segment in segments {
                self.grid.offer(member, block, segment);
            }
        }
        // Only the slices the segments touch, and the whole, fold anew.
        for (key, state) in &self.sets {
            let (Some(index), Some(file)) = (state.usable(), state.bound(member)) else {
                continue;
            };
            let Some(evidence) = self.evidence.get_mut(&(*key, member)) else {
                continue;
            };
            for segment in segments {
                evidence.refold(
                    &self.grid,
                    index,
                    state.whole(file),
                    file,
                    member,
                    segment.range,
                );
            }
        }
    }

    // A slice is proven by checksums cut at its boundaries; the whole
    // member's CRC folds from any.
    fn block_sizes(&self, out: &mut Vec<u64>) {
        out.extend(self.slice_sizes());
    }

    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        let found = self.ordered().into_iter().find_map(|key| {
            let state = self.sets.get(&key)?;
            state.bound(member)?;
            state.usable()?;
            self.evidence.get(&(key, member))
        });
        if let Some(evidence) = found {
            out.extend(evidence.vetoed());
        }
    }

    // Per set, ask for the volumes that serve it and are not yet in hand,
    // in order, until what they are named to carry covers its shortfall.
    fn wants(&mut self, out: &mut Vec<Seek>) {
        let seen = self.damaged_seen();
        let packet = self.smallest_packet();
        for key in self.ordered() {
            let state = &self.sets[&key];
            let wanted = state.needed().max(seen.get(&key).copied().unwrap_or(0));
            let mut shortfall = wanted.saturating_sub(state.recovery.len() as u64);
            for (index, volume) in self.volumes.iter_mut().enumerate() {
                if shortfall == 0 {
                    break;
                }
                if volume.closed || !volume.serves(&key) {
                    continue;
                }
                shortfall = shortfall.saturating_sub(volume.coming(packet));
                volume.ask(VolumeIndex(index as u32), out);
            }
        }
    }

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let mut outcome = RepairOutcome::Clean;
        for key in self.ordered() {
            let coming = self.coming(&key);
            let Some(state) = self.sets.get_mut(&key) else {
                continue;
            };
            let result = match state.concluded() {
                Some(concluded) => concluded,
                None => {
                    let result = repair_set(state, &self.grid, coming, self.memory_limit, access);
                    state.last = Some(result);
                    result.outcome
                }
            };
            outcome = combine(outcome, result);
        }
        self.outcome = Some(outcome);
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.outcome
    }

    fn sets(&self, out: &mut Vec<SetRuling>) {
        for key in self.ordered() {
            let state = &self.sets[&key];
            let last = state.last;
            out.push(SetRuling {
                id: par2_rs::RecoverySetId::from_bytes(key).to_string(),
                slice_size: state.index.as_ref().map_or(0, |index| index.slice_size),
                blocks_reachable: state.recovery.len() as u64 + self.coming(&key),
                damaged_slices: last.map_or(0, |last| last.damaged),
                outcome: last.map(|last| last.outcome),
                read: last.is_some_and(|last| last.read),
            });
        }
    }

    fn blocks_reachable(&self) -> u64 {
        let packet = self.smallest_packet();
        let present: u64 = self
            .sets
            .values()
            .map(|state| state.recovery.len() as u64)
            .sum();
        let coming: u64 = self
            .volumes
            .iter()
            .map(|volume| volume.coming(packet))
            .sum();
        present + coming
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
