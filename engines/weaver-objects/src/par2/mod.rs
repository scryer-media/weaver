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
use par2_rs::packet::{HEADER_SIZE, MAGIC};
use par2_rs::{FileId, Par2FileSet};
use weaver_graph::recovery::{MemberAccess, RepairOutcome, SetRuling};
use weaver_graph::{
    ByteRange, Delivery, Emit, Extent, Kind, Located, MemberId, Namespace, Object, ObjectId,
    Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex,
};

use self::bind::bind_sets;
use self::evidence::Evidence;
use self::repair::{SetRepair, repair_set};
use self::sets::{Location, SetState, Volume};
use self::walker::{Found, Meter};
use crate::grid::{Grid, WHOLE};
use crate::recovery_volume::{Refusal, Shown};

// One job holds no more recovery sets than an NZB holds files.
const MAX_PAR2_RECOVERY_SETS: usize = 100_000;
// What a job may describe, and what one repair may hold in memory when the
// owner grants it no memory of its own.
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
    meter: Meter,
    // A job past the packet or set limits keeps none of its sets.
    refused: bool,
    // The namespace's damage as the object was shown it: a set counts the
    // refusals of the members it binds.
    shown: Shown,
    // The namespace asked for a ruling before any set was found: a volume
    // is read for one.
    seeking: bool,
    // Ranges of its volumes a walk read past as a packet that was none,
    // which it asks for again when it next speaks.
    rewalks: Vec<(VolumeIndex, ByteRange)>,
    output_limit: u64,
    // The repair's workspace when the owner grants it none.
    memory_limit: usize,
}

impl Par2 {
    // A repair's workspace: what it was granted, or the object's own
    // allowance when it was granted none.
    fn workspace(&self, budget: weaver_graph::Budget) -> usize {
        usize::try_from(budget.memory_or(self.memory_limit as u64)).unwrap_or(usize::MAX)
    }

    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: Vec::new(),
            sets: BTreeMap::new(),
            grid: Grid::default(),
            lengths: BTreeMap::new(),
            evidence: BTreeMap::new(),
            meter: Meter::default(),
            refused: false,
            shown: Shown::default(),
            seeking: false,
            rewalks: Vec::new(),
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

    // The most recovery blocks the volumes that serve the set may still
    // bring. Nothing is ruled short while they may.
    fn coming(&self, set: &[u8; 16]) -> u64 {
        self.volumes
            .iter()
            .filter(|volume| volume.serves(set))
            .map(Volume::left)
            .sum()
    }

    // The slices of the set's members that a refused range touches: each
    // will never come, so each needs a block of its own, unless a slice of
    // a member it binds that is neither refused nor came damaged carries
    // the same checksums. A slice is its bytes, and the repair copies them
    // from there.
    fn refused_slices(&self, key: &[u8; 16]) -> u64 {
        let state = &self.sets[key];
        let Some(index) = state.usable().filter(|index| index.slice_size > 0) else {
            return 0;
        };
        let slice = index.slice_size;
        let refused: BTreeSet<(FileId, u64)> = state
            .members()
            .iter()
            .filter_map(|(file, member)| {
                let length = index.file_description(file)?.length;
                Some((*file, length, self.shown.refusals.get(member)?))
            })
            .flat_map(|(file, length, ranges)| {
                ranges
                    .iter()
                    .filter_map(move |range| range.intersect(ByteRange::new(0, length)))
                    .filter(|range| !range.is_empty())
                    .flat_map(move |range| {
                        (range.start / slice..=(range.end - 1) / slice).map(move |at| (file, at))
                    })
            })
            .collect();
        let holds = |file: FileId, at: u64| {
            state.members().get(&file).is_some_and(|member| {
                !refused.contains(&(file, at))
                    && self.evidence.get(&(*key, *member)).is_none_or(|evidence| {
                        evidence.vetoed().nth(at as usize) != Some(SliceVerdict::Damaged)
                    })
            })
        };
        refused
            .iter()
            .filter(|&&(file, at)| {
                !state
                    .alike(file, at)
                    .iter()
                    .any(|&(other, slice)| holds(other, slice))
            })
            .count() as u64
    }

    // The blocks the set can still reach: those in hand and the most its
    // open volumes may bring, so the reach never falls short of the blocks
    // really there. `None` while an open volume that may serve it does not
    // say how many it carries.
    fn reach(&self, key: &[u8; 16]) -> Option<u64> {
        let unknown = self.volumes.iter().any(|volume| {
            // Claimed by its name and nothing of it read yet: what it
            // brings is not known.
            !volume.closed
                && volume.serves(key)
                && (volume.named_blocks.is_none() || volume.arrived.runs().next().is_none())
        });
        (!unknown).then(|| self.sets[key].recovery.len() as u64 + self.coming(key))
    }

    // The set's ruling on what was refused: impossible once the refused
    // slices outrun every block it can still reach. Nothing waits on that,
    // whatever its last repair said.
    fn refused_short(&self, key: &[u8; 16]) -> Option<RepairOutcome> {
        let damaged = self.refused_slices(key);
        if damaged == 0 {
            return None;
        }
        let reachable = self.reach(key)?;
        (damaged > reachable).then_some(RepairOutcome::Impossible { damaged, reachable })
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

    // Whether the namespace shows damage a set that has not read its
    // descriptions may cover: a member, none of the object's own volumes,
    // held damaged or refused, that no set with its descriptions binds.
    // Only then is a volume read for them: on a whole post the set checks
    // nothing and costs nothing.
    fn undescribed_damage(&self) -> bool {
        self.shown.undescribed(|member| {
            self.sets
                .values()
                .any(|state| state.usable().is_some() && state.bound(member).is_some())
        })
    }

    // Damage the namespace shows for the first time makes a set that ruled
    // it checked nothing for want of its descriptions due again: on what
    // it saw then, no volume was worth reading for them; now one is.
    fn damage_shown(&mut self) {
        for state in self.sets.values_mut() {
            if state.usable().is_none() {
                state.damage_shown();
            }
        }
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

    // Whether a set whose last repair came up short still waits: the blocks
    // it needs are not all in hand, and its volumes may bring more.
    fn awaiting_checksums(&self, key: &[u8; 16]) -> bool {
        self.sets[key].usable().is_some()
            && !self.sets[key].checksummed
            && self
                .volumes
                .iter()
                .any(|volume| !volume.closed && volume.serves(key))
    }

    fn waits(&self, key: &[u8; 16], needed: u64) -> bool {
        if self.awaiting_checksums(key) {
            return true;
        }
        let state = &self.sets[key];
        if state.usable().is_none() {
            return self.describing(Some(key)).is_some() && self.undescribed_damage();
        }
        (state.recovery.len() as u64) < needed && self.coming(key) > 0
    }

    // The volume to read for the description of a set that has none, or of
    // any set when none is known: every volume of a set carries its
    // description beside its blocks, so the first still open that may carry
    // the set's packets, an index before any volume and then the fewest
    // blocks first. `None` once none is left to read.
    fn describing(&self, key: Option<&[u8; 16]>) -> Option<usize> {
        let mut order: Vec<usize> = (0..self.volumes.len()).collect();
        order.sort_by_key(|index| (self.volumes[*index].named_blocks, *index));
        order.into_iter().find(|index| {
            let volume = &self.volumes[*index];
            !volume.closed && key.is_none_or(|key| volume.serves(key))
        })
    }

    // Whether no volume of the object has been read through.
    fn unread_through(&self) -> bool {
        !self.volumes.iter().any(Volume::read_through)
    }

    // The volumes, those that name the fewest blocks first.
    fn by_blocks(&self) -> Vec<usize> {
        let mut order: Vec<usize> = (0..self.volumes.len()).collect();
        order.sort_by_key(|index| {
            let named = self.volumes[*index].named_blocks;
            (named.is_none(), named, *index)
        });
        order
    }

    fn take(&mut self, volume: VolumeIndex, found: Vec<Found>) {
        let mut changed_sets = BTreeSet::new();
        for item in found {
            if self.refused {
                return;
            }
            let set = match &item {
                Found::Metadata { set, .. } | Found::Recovery { set, .. } => Some(*set),
                Found::Again(range) => {
                    self.rewalks.push((volume, *range));
                    continue;
                }
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
                Found::Unused | Found::Again(_) => {}
                Found::Metadata {
                    set,
                    at,
                    hash,
                    packet,
                } => {
                    let changed = self
                        .sets
                        .entry(set)
                        .or_insert_with(|| SetState::new((volume, at)))
                        .take_metadata(hash, packet);
                    self.volume(volume).sets.insert(set);
                    if changed {
                        changed_sets.insert(set);
                    }
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
        if !changed_sets.is_empty() {
            self.evidence
                .retain(|(key, _), _| !changed_sets.contains(key));
            for set in changed_sets {
                self.sets
                    .get_mut(&set)
                    .expect("changed set")
                    .rebuild(self.output_limit);
                self.fold_set(set);
            }
        }
    }

    // What the walks read past and must read again, asked of the owner.
    fn ask_again(&mut self, out: &mut Vec<Emit>) {
        out.extend(
            self.rewalks
                .drain(..)
                .map(|(volume, range)| Emit::Again { volume, range }),
        );
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
        out: &mut Vec<Emit>,
    ) {
        let mut found = Vec::new();
        let volume = self.volume(delivery.volume);
        volume.arrived.write(delivery.range(), delivery.provenance);
        volume
            .walker
            .deliver(delivery.offset, delivery.bytes, &mut found);
        self.take(delivery.volume, found);
        self.ask_again(out);
    }

    // The walk reads no packet past the volume's end.
    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        let mut found = Vec::new();
        self.volume(volume).walker.sized(length, &mut found);
        self.take(volume, found);
        self.ask_again(out);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        self.volume(volume).closed = true;
        self.ask_again(out);
    }

    // A loss told again, as a restore tells every loss, adds only what of
    // it the volume did not hold.
    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        let mut found = Vec::new();
        let state = self.volume(volume);
        let fresh = state.lost.hold(range);
        if fresh.is_empty() {
            return;
        }
        for piece in fresh {
            state.walker.lose(piece, &mut found);
        }
        // A recovery packet whose bytes are lost cannot be read back.
        for state in self.sets.values_mut() {
            state
                .recovery
                .retain(|_, at| at.volume != volume || at.range.intersect(range).is_none());
        }
        self.take(volume, found);
        self.ask_again(out);
    }

    fn wants(&self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Every answer means wait. A seek asks for the rest of a volume, and one
    // lost article loses it whole, though the blocks beyond that article
    // still come; what is lost reaches `volume_lost` at its exact range.
    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // What arrived there is to come again, and the blocks it carried; a
    // volume closed over them is open until they do. The walk reads the
    // range again as it comes, so a recovery packet wholly inside it is
    // forgotten and found again from the bytes that come, and counted once.
    // The range is no longer lost: its bytes are coming.
    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        let mut found = Vec::new();
        let Some(state) = self.volumes.get_mut(volume.0 as usize) else {
            return;
        };
        state.arrived.erase(range);
        state.closed = false;
        state.lost.erase(range);
        state.walker.unposted(range, &mut found);
        let mut forgotten = 0;
        for set in self.sets.values_mut() {
            set.recovery.retain(|_, at| {
                let inside = at.volume == volume && range.covers(at.range);
                forgotten += u64::from(inside);
                !inside
            });
        }
        let state = self.volume(volume);
        state.walked_blocks = state.walked_blocks.saturating_sub(forgotten);
        self.take(volume, found);
    }

    // The volume's bytes are all envelope: they stay in the volume's own
    // file, which is the .par2 member itself.
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    // Every PAR2 file is a volume of the one object, numbered as it comes.
    // By the magic its bytes open with, or, before any bytes, by a name
    // that continues the set's: the stem of a volume already held, then a
    // `.vol` name ending in `.par2`.
    fn claim(
        &mut self,
        name: &str,
        first_bytes: &[u8],
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        let ours = if first_bytes.is_empty() {
            self.volumes
                .iter()
                .filter_map(|volume| volume.name.as_deref())
                .any(|held| crate::recovery_volume::same_set(held, name, ".par2"))
        } else {
            first_bytes.get(..MAGIC.len()) == Some(MAGIC.as_slice())
        };
        ours.then_some(VolumeIndex(self.volumes.len() as u32))
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        self.volume(volume).name(name);
        for state in self.sets.values_mut() {
            state.reopen();
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
        if self.shown.bind(namespace, self.id) {
            self.damage_shown();
        }
        let (keys, indexes): (Vec<[u8; 16]>, Vec<&Par2FileSet>) = self
            .ordered()
            .into_iter()
            .filter_map(|key| Some((key, self.sets[&key].usable()?)))
            .unzip();
        let (bound, unnamed) = bind_sets(&indexes, namespace, self.id);
        let mut bound: BTreeMap<_, _> = keys.into_iter().zip(bound).collect();
        let mut moved = BTreeSet::new();
        for (key, state) in &mut self.sets {
            if state.bind(bound.remove(key).unwrap_or_default(), &unnamed) {
                moved.insert(*key);
            }
        }
        // A member a set bound under a name none of its postings answers
        // to the description with, even folded, is recognised: it takes the
        // description's name and length. So is one the set repaired whose
        // good bytes reach past the description: they arrived shifted
        // along, and the rebuilt file ends at the length the set describes.
        // A length booked for articles that never came is not bytes, and
        // proves nothing. One bound under its own name keeps it, and takes
        // the description's length as `Namespace::take_described` says. A
        // member longer than its description by bytes that arrived keeps its
        // length: its posting carries more than the described file.
        for state in self.sets.values() {
            let Some(index) = state.usable() else {
                continue;
            };
            let repaired = matches!(state.concluded(), Some(RepairOutcome::Repaired { .. }));
            for (file, member) in state.members() {
                let Some(description) = index.file_description(file) else {
                    continue;
                };
                let answers = namespace
                    .postings(*member)
                    .any(|record| record.name.eq_ignore_ascii_case(&description.filename));
                let past = ByteRange::new(description.length, u64::MAX);
                let longer = repaired
                    && namespace.get(*member).is_some_and(|record| {
                        record
                            .coverage
                            .overlapping(past)
                            .any(|run| run.provenance.is_verified())
                    });
                if !answers || longer {
                    if namespace
                        .recognise(*member, &description.filename, description.length)
                        .is_some()
                    {
                        renamed.push(*member);
                    }
                    continue;
                }
                namespace.take_described(*member, description.length, repaired);
            }
        }
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
    }

    // Every file a usable set can rebuild that no member is bound to or may
    // be, once each, in set order: the repair reads an unnamed member's
    // slices before any second copy is announced.
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
                if !state.members().contains_key(file)
                    && !state.may_be(description.length)
                    && !out.contains(&named)
                {
                    out.push(named);
                }
            }
        }
    }

    fn undescribed(&self) -> bool {
        self.sets.is_empty() || self.sets.values().any(|state| state.usable().is_none())
    }

    fn covers(&self, member: MemberId) -> bool {
        self.sets
            .values()
            .any(|state| state.bound(member).is_some())
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        if !self
            .grids()
            .into_iter()
            .any(|block| self.grid.matches_on(member, block, segments))
        {
            for state in self.sets.values_mut() {
                if let Some(file) = state.bound(member)
                    && state
                        .usable()
                        .and_then(|index| index.file_description(&file))
                        .is_none_or(|description| {
                            segments.iter().any(|segment| {
                                segment.range.start < description.length
                                    && !segment.range.is_empty()
                            })
                        })
                {
                    state.last = None;
                }
            }
        }
        self.feed_proven(member, segments);
    }

    fn uncovered(&mut self, member: MemberId, _range: ByteRange) {
        self.grid.forget(member);
        let keys: Vec<_> = self
            .sets
            .iter_mut()
            .filter_map(|(key, state)| {
                state.bound(member)?;
                state.last = None;
                Some(*key)
            })
            .collect();
        for key in keys {
            self.fold_set(key);
        }
    }

    fn feed_proven(&mut self, member: MemberId, segments: &[Segment]) {
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

    // Kept for every member of the namespace, bound or not: a set bound to
    // it later counts what was refused before.
    fn refused(&mut self, member: MemberId, range: ByteRange) {
        if self.shown.refused(member, range) == Refusal::Damage {
            self.damage_shown();
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
    // the fewest blocks first, so the least recovery data is fetched, until
    // what they are named to carry covers its shortfall. Every
    // other volume the walk has shown carries recovery blocks is deferred:
    // its head told what it is, and the rest waits until a shortfall wants
    // it. Nothing of the deferral is kept: the same walk says it again on
    // every round, after a restart too. A set asked to rule before it has a
    // description, once the namespace shows damage it may cover, or an
    // object asked before it found any set, waits on one volume that may
    // carry a description, and asks for it, then the next once that one is
    // read through without it.
    fn wants(&self, out: &mut Vec<Seek>) {
        let seen = self.damaged_seen();
        let packet = self.smallest_packet();
        let order = self.by_blocks();
        let mut asked = vec![false; self.volumes.len()];
        if self.sets.is_empty()
            && self.seeking
            && self.unread_through()
            && let Some(index) = self.describing(None)
        {
            asked[index] = true;
            self.volumes[index].ask(VolumeIndex(index as u32), out);
        }
        for key in self.ordered() {
            let state = &self.sets[&key];
            if self.awaiting_checksums(&key) {
                if let Some(index) = self.describing(Some(&key))
                    && !asked[index]
                {
                    asked[index] = true;
                    self.volumes[index].ask(VolumeIndex(index as u32), out);
                }
                continue;
            }
            if state.usable().is_none() {
                if state.short_of().is_some()
                    && self.undescribed_damage()
                    && let Some(index) = self.describing(Some(&key))
                    && !asked[index]
                {
                    asked[index] = true;
                    self.volumes[index].ask(VolumeIndex(index as u32), out);
                }
                continue;
            }
            let wanted = state
                .short_of()
                .unwrap_or(0)
                .max(seen.get(&key).copied().unwrap_or(0));
            let mut shortfall = wanted.saturating_sub(state.recovery.len() as u64);
            for index in &order {
                if shortfall == 0 {
                    break;
                }
                let volume = &self.volumes[*index];
                if volume.closed || !volume.serves(&key) {
                    continue;
                }
                shortfall = shortfall.saturating_sub(volume.coming(packet));
                if !asked[*index] {
                    asked[*index] = true;
                    volume.ask(VolumeIndex(*index as u32), out);
                }
            }
        }
        for (index, volume) in self.volumes.iter().enumerate() {
            if !asked[index] && !volume.closed && volume.shows_recovery() {
                volume.defer(VolumeIndex(index as u32), out);
            }
        }
    }

    fn repair(
        &mut self,
        access: &mut dyn MemberAccess,
        budget: weaver_graph::Budget,
    ) -> RepairOutcome {
        // Its threads are the pool's it runs inside.
        let memory_limit = self.workspace(budget);
        if self.sets.is_empty() && self.unread_through() {
            self.seeking = true;
            return if self.describing(None).is_some() {
                RepairOutcome::Waiting
            } else {
                RepairOutcome::Unread
            };
        }
        let damage = self.undescribed_damage();
        let mut outcome = RepairOutcome::Unread;
        for key in self.ordered() {
            if self.awaiting_checksums(&key) {
                outcome = combine(outcome, RepairOutcome::Waiting);
                continue;
            }
            if let Some(short) = self.refused_short(&key) {
                outcome = combine(outcome, short);
                continue;
            }
            let coming = self.coming(&key);
            let describing = self.describing(Some(&key));
            let Some(state) = self.sets.get_mut(&key) else {
                continue;
            };
            let result = match state.concluded() {
                Some(concluded) => concluded,
                // A set with no description waits on a volume that may carry
                // one while damage shows; with none left to read, or nothing
                // damaged, it checked nothing.
                None if state.usable().is_none() => {
                    let last = match describing {
                        Some(_) if damage => SetRepair::waiting(),
                        _ => SetRepair::unread(),
                    };
                    state.last = Some(last);
                    last.outcome
                }
                None => {
                    let result = repair_set(state, &self.grid, coming, memory_limit, access);
                    state.last = Some(result);
                    result.outcome
                }
            };
            outcome = combine(outcome, result);
        }
        outcome
    }

    // What the sets ruled, folded from Unread as `repair` folds it, each
    // that came up short waiting while it still does. `None` while a set
    // has not ruled since its bindings changed, or once one no longer waits,
    // so it is tried again. With no set found once a volume of its own is
    // read through, nothing was checked: Unread. Before that a set may
    // still be found in the bytes to come, and it has not ruled.
    fn outcome(&self) -> Option<RepairOutcome> {
        if self.sets.is_empty() {
            if !self.unread_through() {
                return Some(RepairOutcome::Unread);
            }
            return self.seeking.then(|| match self.describing(None) {
                Some(_) => RepairOutcome::Waiting,
                None => RepairOutcome::Unread,
            });
        }
        let mut outcome = RepairOutcome::Unread;
        for key in self.ordered() {
            if self.awaiting_checksums(&key) {
                outcome = combine(outcome, RepairOutcome::Waiting);
                continue;
            }
            if let Some(short) = self.refused_short(&key) {
                outcome = combine(outcome, short);
                continue;
            }
            let last = self.sets[&key].last?;
            let ruled = match last.outcome {
                RepairOutcome::Waiting if !self.waits(&key, last.needed) => return None,
                ruled => ruled,
            };
            outcome = combine(outcome, ruled);
        }
        Some(outcome)
    }

    fn sets(&self, out: &mut Vec<SetRuling>) {
        for key in self.ordered() {
            let state = &self.sets[&key];
            let last = state.last;
            let short = self.refused_short(&key);
            let damaged = match short {
                Some(RepairOutcome::Impossible { damaged, .. }) => damaged,
                _ => last.map_or(0, |last| last.damaged),
            };
            out.push(SetRuling {
                id: par2_rs::RecoverySetId::from_bytes(key).to_string(),
                slice_size: state.index.as_ref().map_or(0, |index| index.slice_size),
                blocks_reachable: state.recovery.len() as u64 + self.coming(&key),
                damaged_slices: damaged,
                outcome: short.or(last.map(|last| last.outcome)),
                read: short.is_none() && last.is_some_and(|last| last.read),
            });
        }
    }
}

// Several sets' outcomes as one: the worst kind wins, and outcomes of one
// kind add up. A set that read nothing gives way to any verdict.
pub(crate) fn combine(left: RepairOutcome, right: RepairOutcome) -> RepairOutcome {
    fn rank(outcome: RepairOutcome) -> u8 {
        match outcome {
            RepairOutcome::Unread => 0,
            RepairOutcome::Clean => 1,
            RepairOutcome::Repaired { .. } => 2,
            RepairOutcome::Waiting => 3,
            RepairOutcome::Impossible { .. } => 4,
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
