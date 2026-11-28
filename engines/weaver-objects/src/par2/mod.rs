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
mod catalogue;
mod evidence;
pub(crate) mod facts;
mod repair;
mod sets;
mod walker;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use par2_rs::FileId;
use par2_rs::packet::{HEADER_SIZE, MAGIC};
use weaver_graph::recovery::{
    MemberAccess, RepairOutcome, SetRuling, ShortSet, ShortSets, Unbound, UnboundReason,
};
use weaver_graph::{
    ByteRange, Claim, Delivery, Emit, Extent, Kind, Located, MemberId, Namespace, Object, ObjectId,
    Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex,
};

#[cfg(test)]
thread_local! {
    // Files `absent` asked the set of listed names about.
    static ABSENT_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    // Covered members `covers_any_of` asked the namespace about.
    static COVER_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    // Descriptions a bind compared a posted name with.
    static NAME_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
use self::bind::Bound;
use self::bind::{Bindings, Claims, Fronts, Pass};
use self::catalogue::Catalogue;
use self::evidence::Evidence;
use self::repair::{SetRepair, repair_set};
use self::sets::{Length, Location, SetState, Volume};
use self::walker::{Found, Meter};
use crate::grid::{Grid, WHOLE};
use crate::past_hole::{Packets, Searched, Searches};
use crate::recovery_volume::{Refusal, Shown};

#[cfg(test)]
thread_local! {
    // Refused member files `refused_slices` walked.
    pub(super) static REFUSED_WALKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// One job holds no more recovery sets than an NZB holds files.
const MAX_PAR2_RECOVERY_SETS: usize = 100_000;
// What a job may describe, and what one repair may hold in memory when the
// owner grants it no memory of its own.
const MAX_JOB_BYTES: u64 = 2 * 1024 * 1024 * 1024 * 1024;
const REPAIR_MEMORY_LIMIT: usize = 128 * 1024 * 1024;
// The longest slice a search past a hole reads a recovery packet of
// before any packet has said what the set's slices are: a member whose
// front will not come, with no set known, is read at most this far past
// its hole, about once more than a recovery packet of this slice, for a
// packet whose hash passes. A set of longer slices is found past a hole
// only by a file of it that opens on its own packets.
const UNKNOWN_SLICE: u64 = 16 * 1024 * 1024;

pub struct Par2 {
    id: ObjectId,
    volumes: Vec<Volume>,
    sets: BTreeMap<[u8; 16], SetState>,
    // The set keys in the order their first packets were found: by each
    // set's `first_seen`, which never changes, then its key. Filed where a
    // set comes, and emptied where they all go.
    order: Vec<[u8; 16]>,
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
    // A set was found since the object last spoke: a volume past a lost
    // front that it could not place before may now hold that set's packets.
    learned: bool,
    // What each volume offered past a lost front has shown so far of a
    // packet of these sets. Not kept across a restart: the bytes past the
    // hole are offered again.
    searches: Searches<par2_rs::FileHashState>,
    output_limit: u64,
    // The repair's workspace when the owner grants it none.
    memory_limit: usize,
    // What binding last hashed of each posting's front, by the front's
    // stamp. Not kept across a restart: a restored front is hashed again.
    fronts: Fronts,
    // What binding found of each posting, kept to the next binding, which
    // visits only what changed since. Not kept across a restart: the first
    // binding after one visits every posting.
    claims: Claims,
    // What the usable sets describe, across all of them.
    catalogue: Catalogue,
    // The sets that bind each member: a member any set binds is covered,
    // and its evidence is recorded for those sets alone.
    covered: Cover,
    // While a bind runs: the name each member it named had before.
    renamed_from: BTreeMap<MemberId, String>,
    // The members the last bind came to cover, with their names as it
    // began, until taken.
    newly: Vec<(MemberId, String)>,
    // How many members the last bind moved, as its start was kept.
    #[cfg(test)]
    bind_moves: usize,
    // The sets bound to any member, and those bound to any member or
    // holding any unnamed one: the only sets a binding with nothing new
    // for them can change. Each is filed or unfiled where a set's members
    // or unnamed members change.
    binding: BTreeSet<[u8; 16]>,
    holding: BTreeSet<[u8; 16]>,
    // Each unnamed member a set holds at a free file, by member and by set,
    // filed where the set's held members are filed: an unnamed member's
    // change binds again only the sets holding it, binding it, or with a
    // free file it fits.
    holders: BTreeSet<(MemberId, [u8; 16])>,
    held_by: BTreeSet<([u8; 16], MemberId)>,
    // Sets the next binding binds whatever it finds: their index changed,
    // or an unmatched member came that may be one of their files.
    rebound: BTreeSet<[u8; 16]>,
    // What a binding that weighs only what changed reads beside the
    // claims. Sets whose members or index changed other than by binding:
    // the next binding that binds one weighs all of it. Sets whose last
    // verdict was written or voided since the last binding, and the sets
    // binding a member whose verdict verified as of then: a set whose
    // verdict came to verify, or stopped, is renamed by again.
    disturbed: BTreeSet<[u8; 16]>,
    relast: BTreeSet<[u8; 16]>,
    verified: BTreeSet<[u8; 16]>,
    // The complete members the last binding matched to no set.
    unmatched: Vec<(MemberId, Length)>,
    // Each member a set gave up, under that set: binding passes over the
    // set for it while another set's content answers for it. Not kept
    // across a restart: the set rules again and gives it up again.
    given_up: BTreeSet<([u8; 16], MemberId)>,
    // Room a feed lists its grids in: each feed empties it and lists them
    // afresh, so nothing in it outlives the feed that wrote it.
    grids: Vec<u64>,
    // How many member fronts binding has hashed.
    #[cfg(test)]
    digests: usize,
    // How many times binding matched a member against a set, and how many
    // times it bound a set.
    #[cfg(test)]
    set_checks: usize,
    #[cfg(test)]
    set_binds: usize,
    // How many set-member entries binding visited: files and unnamed
    // members weighed, members renamed by and lengths read.
    #[cfg(test)]
    entries: usize,
    // Binding matches every member against every set and binds every set,
    // weighing every file and unnamed member, renaming by every member and
    // listing every length: binding as it was before it weighed only what
    // changed, kept to compare with.
    #[cfg(test)]
    scanning: bool,
    // How many times listing the grids needed new room.
    #[cfg(test)]
    grid_room: usize,
    // How many times the volumes were sorted.
    #[cfg(test)]
    volume_sorts: std::sync::atomic::AtomicUsize,
    // How many times binding listed the bound members' lengths afresh.
    #[cfg(test)]
    length_lists: usize,
    // How many times a set was put in its place in the order.
    #[cfg(test)]
    set_orderings: std::sync::atomic::AtomicUsize,
    // How many members' evidence folds built afresh.
    #[cfg(test)]
    members_folded: usize,
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
            order: Vec::new(),
            grid: Grid::default(),
            lengths: BTreeMap::new(),
            evidence: BTreeMap::new(),
            meter: Meter::default(),
            refused: false,
            shown: Shown::default(),
            seeking: false,
            rewalks: Vec::new(),
            learned: false,
            searches: Searches::default(),
            output_limit: MAX_JOB_BYTES,
            memory_limit: REPAIR_MEMORY_LIMIT,
            fronts: Fronts::new(),
            claims: Claims::default(),
            catalogue: Catalogue::default(),
            covered: Cover::default(),
            renamed_from: BTreeMap::new(),
            newly: Vec::new(),
            #[cfg(test)]
            bind_moves: 0,
            binding: BTreeSet::new(),
            holding: BTreeSet::new(),
            holders: BTreeSet::new(),
            held_by: BTreeSet::new(),
            rebound: BTreeSet::new(),
            disturbed: BTreeSet::new(),
            relast: BTreeSet::new(),
            verified: BTreeSet::new(),
            unmatched: Vec::new(),
            given_up: BTreeSet::new(),
            grids: Vec::new(),
            #[cfg(test)]
            digests: 0,
            #[cfg(test)]
            set_checks: 0,
            #[cfg(test)]
            set_binds: 0,
            #[cfg(test)]
            entries: 0,
            #[cfg(test)]
            scanning: false,
            #[cfg(test)]
            grid_room: 0,
            #[cfg(test)]
            volume_sorts: Default::default(),
            #[cfg(test)]
            length_lists: 0,
            #[cfg(test)]
            set_orderings: Default::default(),
            #[cfg(test)]
            members_folded: 0,
        }
    }

    // Set keys in the order their first packets were found.
    fn ordered(&self) -> &[[u8; 16]] {
        &self.order
    }

    // The set `key` was filed: a member another set binds, whose front a
    // file of this set has, is described by this set too from now on, and
    // what arrived of it is asked for again on this set's slices.
    fn describes_anew(&mut self, key: [u8; 16]) {
        if !self.catalogue.shares() {
            return;
        }
        let Some(index) = self.sets.get(&key).and_then(SetState::usable) else {
            return;
        };
        for described in index.files.values() {
            let front = sets::Front::described(described.length, described.hash_16k);
            for (_, other) in self.catalogue.fronted(front) {
                let Some(state) = self.sets.get(&other).filter(|_| other != key) else {
                    continue;
                };
                for file in state.fronted(&front) {
                    if let Some(member) = state.members().get(file) {
                        self.covered.unfed.insert(*member);
                    }
                }
            }
        }
    }

    // A set the object did not have, filed by where its first packet was
    // found.
    fn file_set(&mut self, key: [u8; 16], state: SetState) {
        let place = (state.first_seen, key);
        if let Some(index) = state.usable() {
            self.catalogue.file(place, index);
        }
        let was = self.sets.insert(key, state);
        self.covered.rebound(
            key,
            was.iter().flat_map(|was| was.members().values()),
            self.sets[&key].members().values(),
        );
        self.held(key);
        self.rebound.insert(key);
        self.disturbed.insert(key);
        self.relast.insert(key);
        self.describes_anew(key);
        if let Some(was) = &was
            && let Some(index) = was.usable()
        {
            self.catalogue.unfile((was.first_seen, key), index);
        }
        if was.is_none() {
            #[cfg(test)]
            self.set_orderings
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let sets = &self.sets;
            let at = self
                .order
                .partition_point(|other| (sets[other].first_seen, *other) < place);
            self.order.insert(at, key);
        }
    }

    // Files the set among those binding members, and those holding any,
    // by what it holds now.
    fn held(&mut self, key: [u8; 16]) {
        let (binds, holds) = self.sets.get(&key).map_or((false, false), |state| {
            let binds = !state.members().is_empty();
            (binds, binds || !state.unnamed().is_empty())
        });
        if binds {
            self.binding.insert(key);
        } else {
            self.binding.remove(&key);
        }
        if holds {
            self.holding.insert(key);
        } else {
            self.holding.remove(&key);
        }
        let now: BTreeSet<MemberId> = self
            .sets
            .get(&key)
            .map(|state| state.unnamed().iter().map(|(member, _)| *member).collect())
            .unwrap_or_default();
        let was: Vec<MemberId> = self
            .held_by
            .range((key, MemberId(0))..=(key, MemberId(u32::MAX)))
            .map(|(_, member)| *member)
            .collect();
        for member in &was {
            if !now.contains(member) {
                self.held_by.remove(&(key, *member));
                self.holders.remove(&(*member, key));
            }
        }
        for member in now {
            if self.held_by.insert((key, member)) {
                self.holders.insert((member, key));
            }
        }
    }

    // The set under `key`, new from where its first packet was found when
    // the object does not have it.
    fn found_set(&mut self, key: [u8; 16], first_seen: (VolumeIndex, u64)) -> &mut SetState {
        if !self.sets.contains_key(&key) {
            self.file_set(key, SetState::new(first_seen));
        }
        self.sets.get_mut(&key).expect("filed set")
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
        let packet = self.smallest_packet();
        self.volumes
            .iter()
            .filter(|volume| volume.serves(set))
            .map(|volume| volume.left(packet))
            .fold(0, u64::saturating_add)
    }

    // The blocks still to come that can be counted, for a figure that is
    // shown: a volume with no bound known yet adds none to it.
    fn counted(&self, set: &[u8; 16]) -> u64 {
        let packet = self.smallest_packet();
        self.volumes
            .iter()
            .filter(|volume| volume.serves(set))
            .map(|volume| volume.left(packet))
            .filter(|&left| left != u64::MAX)
            .fold(0, u64::saturating_add)
    }

    // The slices of the set's members that a refused range touches: each
    // will never come, so each needs a block of its own, unless a slice of
    // a member it binds that is neither refused nor came damaged carries
    // the same checksums. A slice is its bytes, and the repair copies them
    // from there. A refused slice the member's checksums prove intact came
    // after all, by a repair or by a further posting folded into it, and
    // needs nothing.
    fn refused_slices(&self, key: &[u8; 16]) -> u64 {
        let state = &self.sets[key];
        let Some(index) = state.usable().filter(|index| index.slice_size > 0) else {
            return 0;
        };
        let slice = index.slice_size;
        let intact = |member: MemberId, at: u64| {
            self.evidence.get(&(*key, member)).is_some_and(|evidence| {
                evidence.vetoed().nth(at as usize) == Some(SliceVerdict::Intact)
            })
        };
        // Walked by the refused members, each by the files the set binds it
        // to: a set none of whose members was refused walks nothing.
        let walk = self.shown.refusals.iter().flat_map(|(member, ranges)| {
            state
                .files_of(*member)
                .map(move |file| (file, *member, ranges))
        });
        #[cfg(test)]
        let walk = walk.inspect(|_| {
            REFUSED_WALKED.with(|walked| walked.set(walked.get() + 1));
        });
        let refused: BTreeSet<(FileId, u64)> = walk
            .filter_map(|(file, member, ranges)| {
                let length = index.file_description(&file)?.length;
                Some((file, member, length, ranges))
            })
            .flat_map(|(file, member, length, ranges)| {
                ranges
                    .iter()
                    .filter_map(move |range| range.intersect(ByteRange::new(0, length)))
                    .filter(|range| !range.is_empty())
                    .flat_map(move |range| {
                        (range.start / slice..=(range.end - 1) / slice).map(move |at| (file, at))
                    })
                    .filter(move |&(_, at)| !intact(member, at))
            })
            .collect();
        // The walk it replaced: every member the set binds, asked whether
        // it was refused.
        #[cfg(test)]
        {
            let by_members: BTreeSet<(FileId, u64)> = state
                .members()
                .iter()
                .filter_map(|(file, member)| {
                    let length = index.file_description(file)?.length;
                    Some((*file, *member, length, self.shown.refusals.get(member)?))
                })
                .flat_map(|(file, member, length, ranges)| {
                    ranges
                        .iter()
                        .filter_map(move |range| range.intersect(ByteRange::new(0, length)))
                        .filter(|range| !range.is_empty())
                        .flat_map(move |range| {
                            (range.start / slice..=(range.end - 1) / slice)
                                .map(move |at| (file, at))
                        })
                        .filter(move |&(_, at)| !intact(member, at))
                })
                .collect();
            assert_eq!(refused, by_members, "refused slices walked by the refusals");
        }
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
        (!unknown).then(|| (self.sets[key].recovery.len() as u64).saturating_add(self.coming(key)))
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

    // The slice sizes of the sets that describe the member, onto `out`:
    // each set that binds it, and each other set describing a file of its
    // front, which takes it if the first cannot rebuild it. Its evidence
    // is cut and kept at these, so the set it moves to holds what the
    // first did. A size may be named more than once.
    fn described_sizes(&self, member: MemberId, out: &mut Vec<u64>) {
        for key in self.covered.sets(member) {
            let Some(state) = self.sets.get(key) else {
                continue;
            };
            let Some(index) = state.index.as_ref() else {
                continue;
            };
            out.push(index.slice_size);
            if !self.catalogue.shares() {
                continue;
            }
            let described = state
                .bound(member)
                .and_then(|file| index.file_description(&file));
            if let Some(described) = described {
                let front = sets::Front::described(described.length, described.hash_16k);
                out.extend(
                    self.catalogue
                        .fronted(front)
                        .filter_map(|(_, other)| self.sets.get(&other)?.index.as_ref())
                        .map(|index| index.slice_size),
                );
            }
        }
    }

    // Every grid the member's evidence is folded on, each once, smallest
    // first: the slices of each set that describes it, and the whole
    // member. A set that comes to describe it later is fed what came before
    // (`unfed`), so no set's slices are kept for a member it does not
    // describe. Listed in the object's own room, which the caller hands
    // back when the feed is done.
    fn list_grids(&mut self, member: MemberId) -> Vec<u64> {
        let mut grids = std::mem::take(&mut self.grids);
        #[cfg(test)]
        let room = grids.capacity();
        grids.clear();
        self.described_sizes(member, &mut grids);
        grids.push(WHOLE);
        grids.sort_unstable();
        grids.dedup();
        #[cfg(test)]
        {
            self.grid_room += usize::from(grids.capacity() != room);
        }
        grids
    }

    // Offer the segments to every grid, then fold anew only the slices
    // they touch, and the whole.
    fn offer(&mut self, member: MemberId, segments: &[Segment], grids: &[u64]) {
        for block in grids {
            self.grid.offer_all(member, *block, segments);
        }
        for key in self.covered.sets(member) {
            let Some(state) = self.sets.get(key) else {
                continue;
            };
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

    // The slices the evidence shows the set needs a block for, over its
    // members: those the checksums say are damaged, and those a refusal
    // says will never come. The two never overlap: a damaged slice was
    // folded, so its bytes are held, and a refused one counts only while
    // they are not.
    fn needs_seen(&self, key: &[u8; 16]) -> (u64, u64) {
        self.evidence
            .range((*key, MemberId(0))..=(*key, MemberId(u32::MAX)))
            .fold((0, 0), |(damaged, lost), (_, evidence)| {
                (damaged + evidence.damaged(), lost + evidence.lost())
            })
    }

    // Mark the slices of the set's `file`, bound to `member`, that a refused
    // range touches, so the set asks for their blocks while the rest of the
    // payload still comes, as `refused_slices` counts them for its ruling: a
    // slice whose checksums an alike slice of the set carries, one held and
    // not damaged, is copied from it and needs none. Each slice is weighed
    // once, when a refusal first touches it; what its fold says later moves
    // it in and out of the count.
    fn mark_refused(&mut self, key: [u8; 16], file: FileId, member: MemberId, range: ByteRange) {
        let Some(state) = self.sets.get(&key) else {
            return;
        };
        let Some(index) = state.usable().filter(|index| index.slice_size > 0) else {
            return;
        };
        let Some(length) = index.file_description(&file).map(|found| found.length) else {
            return;
        };
        let Some(range) = range
            .intersect(ByteRange::new(0, length))
            .filter(|range| !range.is_empty())
        else {
            return;
        };
        let Some(evidence) = self.evidence.get(&(key, member)) else {
            return;
        };
        let slice = index.slice_size;
        let extent =
            |at: u64, length: u64| ByteRange::new(at * slice, (at * slice + slice).min(length));
        let holds = |other: FileId, at: u64, own: u64| {
            if other == file && at == own {
                return false;
            }
            let (Some(&holder), Some(described)) =
                (state.members().get(&other), index.file_description(&other))
            else {
                return false;
            };
            let bytes = extent(at, described.length);
            let damaged = self
                .evidence
                .get(&(key, holder))
                .is_some_and(|evidence| evidence.damaged_at(at));
            let refused = self.shown.refusals.get(&holder).is_some_and(|ranges| {
                ranges
                    .iter()
                    .any(|refused| refused.intersect(bytes).is_some_and(|hit| !hit.is_empty()))
            });
            !damaged && (!refused || self.grid.fold(holder, slice, at, bytes).is_some())
        };
        let marks: Vec<(usize, bool)> = (range.start / slice..=(range.end - 1) / slice)
            .filter(|at| !evidence.marked(*at as usize))
            .filter(|at| {
                !state
                    .alike(file, *at)
                    .iter()
                    .any(|&(other, other_at)| holds(other, other_at, *at))
            })
            .map(|at| {
                let held = self
                    .grid
                    .fold(member, slice, at, extent(at, length))
                    .is_some();
                (at as usize, held)
            })
            .collect();
        if let Some(evidence) = self.evidence.get_mut(&(key, member)) {
            for (at, held) in marks {
                evidence.refuse(at, held);
            }
        }
    }

    // Mark what was refused of each member the set binds, as a fold builds
    // its evidence afresh.
    fn mark_set_refused(&mut self, key: [u8; 16]) {
        let Some(state) = self.sets.get(&key) else {
            return;
        };
        let refused: Vec<(FileId, MemberId, ByteRange)> = state
            .members()
            .iter()
            .filter_map(|(file, member)| Some((*file, *member, self.shown.refusals.get(member)?)))
            .flat_map(|(file, member, ranges)| {
                ranges.iter().map(move |range| (file, member, *range))
            })
            .collect();
        for (file, member, range) in refused {
            self.mark_refused(key, file, member, range);
        }
    }

    // Fold every bound member of the set afresh: its index, its bindings or
    // its members' lengths changed, and each verdict reads all three.
    pub(super) fn fold_set(&mut self, key: [u8; 16]) {
        if let Some(state) = self.sets.get_mut(&key) {
            state.take_unfolded();
        }
        while let Some((&at, _)) = self
            .evidence
            .range((key, MemberId(0))..=(key, MemberId(u32::MAX)))
            .next()
        {
            self.evidence.remove(&at);
        }
        let Some(state) = self.sets.get(&key) else {
            return;
        };
        let Some(index) = state.usable() else {
            return;
        };
        for (file, member) in state.members() {
            self.evidence.entry((key, *member)).or_insert_with(|| {
                #[cfg(test)]
                {
                    self.members_folded += 1;
                }
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
        self.mark_set_refused(key);
    }

    // Fold afresh only what of the set may have changed: `members`, whose
    // length or file's checksums changed, and each member whose file the
    // set changed since it was last folded. A member's evidence reads its
    // file's description, checksums and whole CRC32, its length and its
    // folds, and nothing of any other member, so the rest stand as
    // `fold_set` would build them. What was refused of a member is weighed
    // against the slices other members hold, so a set any refusal touches
    // is folded whole.
    fn fold_members(&mut self, key: [u8; 16], members: impl IntoIterator<Item = MemberId>) {
        #[cfg(test)]
        let reference = self.folded_whole(key);
        let Some(state) = self.sets.get_mut(&key) else {
            return self.fold_set(key);
        };
        let mut members: BTreeSet<MemberId> = members.into_iter().collect();
        members.append(&mut state.take_unfolded());
        let state = &self.sets[&key];
        let refused = self
            .shown
            .refusals
            .keys()
            .any(|member| members.contains(member) || state.bound(*member).is_some());
        let Some(index) = state.usable().filter(|_| !refused) else {
            return self.fold_set(key);
        };
        for member in members {
            self.evidence.remove(&(key, member));
            let Some(file) = state.bound(member) else {
                continue;
            };
            #[cfg(test)]
            {
                self.members_folded += 1;
            }
            let evidence = Evidence::of(
                &self.grid,
                index,
                state.whole(file),
                file,
                member,
                self.lengths.get(&member).copied(),
            );
            self.evidence.insert((key, member), evidence);
        }
        #[cfg(test)]
        assert_eq!(
            self.evidence
                .range((key, MemberId(0))..=(key, MemberId(u32::MAX)))
                .collect::<Vec<_>>(),
            reference.iter().collect::<Vec<_>>(),
            "a fold of what changed builds what folding the set whole does"
        );
    }

    // The set's evidence as `fold_set` builds it, the object left as it was:
    // what a fold of what changed is held to.
    #[cfg(test)]
    fn folded_whole(&mut self, key: [u8; 16]) -> BTreeMap<([u8; 16], MemberId), Evidence> {
        let range = (key, MemberId(0))..=(key, MemberId(u32::MAX));
        let kept: BTreeMap<_, _> = self
            .evidence
            .range(range.clone())
            .map(|(at, evidence)| (*at, evidence.clone()))
            .collect();
        let unfolded = self.sets.get(&key).map(|state| state.unfolded().clone());
        let folds = self.members_folded;
        self.fold_set(key);
        let folded = self
            .evidence
            .range(range.clone())
            .map(|(at, evidence)| (*at, evidence.clone()))
            .collect();
        let held: Vec<_> = self.evidence.range(range).map(|(at, _)| *at).collect();
        for at in held {
            self.evidence.remove(&at);
        }
        self.evidence.extend(kept);
        if let (Some(state), Some(unfolded)) = (self.sets.get_mut(&key), unfolded) {
            state.restore_unfolded(unfolded);
        }
        self.members_folded = folds;
        folded
    }

    // Whether the set stands ruled that it cannot rebuild what it binds: by
    // its last repair, or by what was refused of its members.
    fn short(&self, key: &[u8; 16]) -> bool {
        matches!(
            self.sets.get(key).and_then(SetState::concluded),
            Some(RepairOutcome::Impossible { .. })
        ) || self.refused_short(key).is_some()
    }

    // The sets among `keys` that bind a member and stand short give their
    // members up. Asked only while two usable sets describe a file of one
    // front: otherwise no member has another set to move to, and nothing
    // is weighed. Whether any member moved.
    fn settle_short(&mut self, keys: impl IntoIterator<Item = [u8; 16]>) -> bool {
        if !self.catalogue.shares() {
            return false;
        }
        let mut moved = false;
        for key in keys {
            if self.binding.contains(&key) && self.short(&key) {
                moved |= self.give_up(key);
            }
        }
        moved
    }

    // The set ruled it cannot rebuild what it binds: it gives each member
    // up, and each moves to the set binding names next for it
    // (`bind::next_holder`), whose file for it is free. The set gave up
    // nothing it wrote: a set that rules so writes nothing. Each set that
    // lost or gained a member rules again, over what it binds now, and the
    // next binding keeps the move, since it passes over the set for the
    // member too. Whether any member moved.
    fn give_up(&mut self, key: [u8; 16]) -> bool {
        let Some(state) = self.sets.get(&key) else {
            return false;
        };
        let members: Vec<(FileId, MemberId)> = state
            .members()
            .iter()
            .map(|(file, member)| (*file, *member))
            .collect();
        let mut changed: BTreeMap<[u8; 16], Bindings> = BTreeMap::new();
        for (file, member) in members {
            self.given_up.insert((key, member));
            self.claims.given_up(member);
            let Some((to, at)) = bind::next_holder(
                &self.sets,
                &self.catalogue,
                (key, file),
                member,
                &self.given_up,
            ) else {
                continue;
            };
            if to == key {
                continue;
            }
            let sets = &self.sets;
            let taken = changed
                .entry(to)
                .or_insert_with(|| sets[&to].members().clone());
            if taken.contains_key(&at) {
                continue;
            }
            taken.insert(at, member);
            changed
                .entry(key)
                .or_insert_with(|| sets[&key].members().clone())
                .remove(&file);
        }
        let mut moved = BTreeSet::new();
        #[cfg(test)]
        let weigh = if self.scanning {
            Weigh::Whole(&self.unmatched)
        } else {
            Weigh::Kept(self.claims.unnamed())
        };
        #[cfg(not(test))]
        let weigh = Weigh::Kept(self.claims.unnamed());
        for (set, given) in changed {
            self.disturbed.insert(set);
            rebind(
                &mut self.sets,
                &mut self.covered,
                set,
                &given,
                &weigh,
                &mut moved,
            );
        }
        for set in &moved {
            self.relast.insert(*set);
            self.held(*set);
            self.fold_set(*set);
        }
        #[cfg(test)]
        self.assert_filed();
        !moved.is_empty()
    }

    // The volume a described set waits on for the slice checksums it lacks:
    // the one to read for them, while one that may carry them is still open.
    fn awaiting_checksums(&self, key: &[u8; 16]) -> Option<usize> {
        let state = &self.sets[key];
        if state.usable().is_none() || state.checksummed {
            return None;
        }
        self.describing(Some(key))
    }

    fn waits(&self, key: &[u8; 16], needed: u64) -> bool {
        if self.awaiting_checksums(key).is_some() {
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
        self.volumes
            .iter()
            .enumerate()
            .filter(|(_, volume)| !volume.closed && key.is_none_or(|key| volume.serves(key)))
            .min_by_key(|(index, volume)| (volume.named_blocks, *index))
            .map(|(index, _)| index)
    }

    // Whether no volume of the object has been read through.
    fn unread_through(&self) -> bool {
        !self.volumes.iter().any(Volume::read_through)
    }

    // The volumes, those that name the fewest blocks first.
    fn by_blocks(&self) -> Vec<usize> {
        #[cfg(test)]
        self.volume_sorts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut order: Vec<usize> = (0..self.volumes.len()).collect();
        order.sort_by_key(|index| {
            let named = self.volumes[*index].named_blocks;
            (named.is_none(), named, *index)
        });
        order
    }

    // Each usable set's bindings over the namespace, by key, as the scan
    // finds them.
    #[cfg(test)]
    fn scan_sets(&mut self, namespace: &Namespace) -> Bound<BTreeMap<[u8; 16], Bindings>> {
        let (keys, usable): (Vec<[u8; 16]>, Vec<&SetState>) = self
            .order
            .iter()
            .map(|key| (*key, &self.sets[key]))
            .filter(|(_, state)| state.usable().is_some())
            .unzip();
        // The scan goes around the claims: the next binding by them
        // visits every posting.
        self.claims.stale();
        let given_up = &self.given_up;
        let found = bind::bind_sets(&usable, namespace, self.id, &mut self.fronts, |at, m| {
            given_up.contains(&(keys[at], m))
        });
        Bound {
            sets: keys.into_iter().zip(found.sets).collect(),
            unnamed: found.unnamed,
            digests: found.digests,
            checks: found.checks,
        }
    }

    // Each usable set's bindings over the namespace, kept by the claims:
    // what changed of them since the last binding.
    fn bind_sets(&mut self, namespace: &Namespace) -> Pass {
        // A set filed, unfiled or cleared may answer for any posting.
        if self.catalogue.take_changed() {
            self.claims.stale();
        }
        #[cfg(test)]
        let before = self.fronts.clone();
        let found = self.claims.bind(
            &self.sets,
            &self.catalogue,
            namespace,
            self.id,
            &mut self.fronts,
            &self.given_up,
        );
        // What binding every posting comes to, from the same fronts.
        #[cfg(test)]
        {
            let mut fronts = before;
            let whole = bind::bind_catalogued(
                &self.sets,
                &self.catalogue,
                namespace,
                self.id,
                &mut fronts,
                &self.given_up,
            );
            assert_eq!(*self.claims.bindings(), whole.sets, "bindings");
            assert_eq!(self.claims.unnamed().list(), whole.unnamed, "unnamed");
            assert_eq!(self.fronts, fronts, "fronts");
        }
        found
    }

    // `absent` as it read before it kept a set of what is listed: each
    // file looked for along `out`.
    #[cfg(test)]
    fn absent_by_scan(&self, out: &mut Vec<(String, u64)>) {
        for key in self.ordered() {
            let state = &self.sets[key];
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

    // What binding keeps beside the sets agrees with the sets.
    #[cfg(test)]
    fn assert_filed(&self) {
        let binding: BTreeSet<_> = self
            .sets
            .iter()
            .filter(|(_, state)| !state.members().is_empty())
            .map(|(key, _)| *key)
            .collect();
        let holding: BTreeSet<_> = self
            .sets
            .iter()
            .filter(|(_, state)| !state.members().is_empty() || !state.unnamed().is_empty())
            .map(|(key, _)| *key)
            .collect();
        let mut covered = Cover::default();
        for (key, state) in &self.sets {
            covered.rebound(*key, [].iter(), state.members().values());
        }
        assert_eq!(self.binding, binding);
        assert_eq!(self.holding, holding);
        let held_by: BTreeSet<([u8; 16], MemberId)> = self
            .sets
            .iter()
            .flat_map(|(key, state)| state.unnamed().iter().map(|(member, _)| (*key, *member)))
            .collect();
        assert_eq!(self.held_by, held_by, "held unnamed members by set");
        let holders: BTreeSet<_> = held_by
            .iter()
            .map(|(key, member)| (*member, *key))
            .collect();
        assert_eq!(self.holders, holders, "held unnamed members by member");
        assert_eq!(self.covered.by, covered.by);
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
                Found::Unused | Found::Failed => None,
            };
            let new_set = set.is_some_and(|set| !self.sets.contains_key(&set));
            if !self.meter.charge(&item) || (new_set && self.sets.len() >= MAX_PAR2_RECOVERY_SETS) {
                self.refused = true;
                self.sets.clear();
                self.order.clear();
                self.evidence.clear();
                self.lengths.clear();
                self.catalogue.clear();
                self.covered.clear();
                self.binding.clear();
                self.holding.clear();
                self.holders.clear();
                self.held_by.clear();
                self.rebound.clear();
                self.disturbed.clear();
                self.relast.clear();
                self.verified.clear();
                self.unmatched.clear();
                self.given_up.clear();
                return;
            }
            self.learned |= new_set;
            match item {
                Found::Unused | Found::Failed | Found::Again(_) => {}
                Found::Metadata {
                    set,
                    at,
                    hash,
                    packet,
                } => {
                    let changed = self
                        .found_set(set, (volume, at))
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
                    self.found_set(set, (volume, range.start))
                        .recovery
                        .entry(exponent)
                        .or_insert(Location { volume, range });
                    let carrier = self.volume(volume);
                    carrier.walked_blocks += 1;
                    carrier.sets.insert(set);
                }
            }
        }
        // The sets whose index became usable now.
        let mut usable = BTreeSet::new();
        for set in changed_sets {
            let state = self.sets.get_mut(&set).expect("changed set");
            let place = (state.first_seen, set);
            let was_usable = state.usable().is_some();
            let rebuilt = state.rebuild(self.output_limit);
            // A packet that changes nothing binding reads, as the slice
            // checksums an index carries after its descriptions, leaves
            // the catalogue and every binding as they stand: only its
            // verdict and the folds of the files it checks are read anew.
            if rebuilt.binds_alike {
                let state = &self.sets[&set];
                let members: Vec<MemberId> = rebuilt
                    .checked
                    .iter()
                    .filter_map(|file| state.members().get(file).copied())
                    .collect();
                self.relast.insert(set);
                self.fold_members(set, members);
                continue;
            }
            if let sets::Before::Index(index) = &rebuilt.before {
                self.catalogue.unfile(place, index);
            }
            let state = &self.sets[&set];
            if let Some(index) = state.usable() {
                match &rebuilt.before {
                    sets::Before::Described(files) => self.catalogue.describe(place, index, files),
                    sets::Before::Index(_) => self.catalogue.file(place, index),
                    sets::Before::Unfiled => {
                        self.catalogue.file(place, index);
                        usable.insert(set);
                    }
                }
            } else {
                debug_assert!(!matches!(rebuilt.before, sets::Before::Described(_)));
                // A set that was not usable before the take and still is
                // not, holding nothing a binding, a fold or a verdict
                // reads, binds and folds to nothing again: binding it gives
                // it no member, and it has no evidence to drop. One that
                // was usable, or still holds any of these, binds again.
                if !was_usable
                    && state.holds_nothing()
                    && !self.verified.contains(&set)
                    && self
                        .evidence
                        .range((set, MemberId(0))..=(set, MemberId(u32::MAX)))
                        .next()
                        .is_none()
                {
                    continue;
                }
            }
            self.rebound.insert(set);
            self.disturbed.insert(set);
            self.relast.insert(set);
            self.fold_set(set);
        }
        for set in &usable {
            self.describes_anew(*set);
        }
        // A set that came to describe a front another set binds may take
        // what that set, standing short, gives up: only the sets describing
        // one of its fronts are weighed.
        if self.catalogue.shares() {
            let mut sharing = BTreeSet::new();
            for set in &usable {
                let Some(index) = self.sets.get(set).and_then(SetState::usable) else {
                    continue;
                };
                for description in index.files.values() {
                    let front = sets::Front::described(description.length, description.hash_16k);
                    sharing.extend(
                        self.catalogue
                            .fronted(front)
                            .map(|(_, key)| key)
                            .filter(|key| !usable.contains(key)),
                    );
                }
            }
            self.settle_short(sharing);
        }
    }

    // What the walks read past and must read again, asked of the owner.
    fn ask_again(&mut self, out: &mut Vec<Emit>) {
        out.extend(
            self.rewalks
                .drain(..)
                .map(|(volume, range)| Emit::Again { volume, range }),
        );
        if std::mem::take(&mut self.learned) {
            out.push(Emit::Learned);
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
        let state = self.volume(volume);
        state.length = Some(length);
        state.walker.sized(length, &mut found);
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
        // A loss lowers what the sets the volume serves can reach: one it
        // leaves short gives its members up.
        if self.catalogue.shares() {
            let served: Vec<[u8; 16]> = self.volumes[volume.0 as usize]
                .sets
                .iter()
                .copied()
                .collect();
            self.settle_short(served);
        }
        self.ask_again(out);
    }

    fn wants(&self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Every answer means wait. A seek asks for the rest of a volume, and one
    // lost article loses it whole, though the blocks beyond that article
    // still come; what is lost reaches `volume_lost` at its exact range.
    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // Nothing past each walk's front is in the facts: a packet streamed when
    // the checkpoint was taken, the bytes held past the front, and what
    // waited while the object was out for a repair. It is asked for again,
    // and walked as what of it arrived comes.
    fn restored(&mut self, out: &mut Vec<Emit>) {
        for (index, volume) in self.volumes.iter().enumerate() {
            let mut past = weaver_graph::Coverage::new();
            past.write(
                ByteRange::new(volume.walker.front(), u64::MAX),
                weaver_graph::Provenance::Fresh,
            );
            for lost in volume.walker.lost() {
                past.erase(lost);
            }
            let volume = VolumeIndex(index as u32);
            self.rewalks
                .extend(past.runs().map(|run| (volume, run.range)));
        }
        self.ask_again(out);
    }

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
    // `.vol` name ending in `.par2`. A member whose front will not come is
    // one by a packet in its clean bytes past the hole, of a set this
    // object holds, its hash passing: searched for as those bytes are
    // handed over, not yet while the packet has not passed whole, and
    // none once the search's bound is past (`past_hole`). Magic alone, or a
    // packet of another set, claims nothing. An object that holds no set
    // yet, built to judge such a member when no PAR2 file opened on its own
    // packets, takes a packet of any set whose hash passes: that packet
    // founds the set, whose other files are then claimed by theirs.
    fn claim(&mut self, name: &str, clean: weaver_graph::Clean<'_>, _length: Option<u64>) -> Claim {
        let first_bytes = clean.front();
        let place = VolumeIndex(self.volumes.len() as u32);
        if clean.at > 0 {
            let format = SetPackets { sets: &self.sets };
            return match self.searches.read(name, (clean.at, clean.bytes), &format) {
                Searched::Found => Claim::Places(vec![place]),
                Searched::More => Claim::NotYet(Vec::new()),
                Searched::Never => Claim::default(),
            };
        }
        let ours = if first_bytes.is_empty() {
            self.volumes
                .iter()
                .filter_map(|volume| volume.name.as_deref())
                .any(|held| crate::recovery_volume::same_set(held, name, ".par2"))
        } else {
            first_bytes.get(..MAGIC.len()) == Some(MAGIC.as_slice())
        };
        ours.then_some(place).into_iter().collect()
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], length: Option<u64>) {
        self.searches.end(name);
        let state = self.volume(volume);
        state.name(name);
        state.length = length.or(state.length);
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

    // Each walk holds what comes past its front, and notes what it cannot
    // hold to ask for again: a range asked for again mid-replay waits.
    fn again_waits(&self) -> bool {
        true
    }
}

// PAR2 packets as a search past a hole reads them: of a set the object
// holds, and no longer than the longest such a set holds, a recovery
// packet of its slice size or the longest metadata the walker reads. An
// object that holds no set reads a packet of any set, its slice size not
// known: no longer than a recovery packet of UNKNOWN_SLICE bytes.
struct SetPackets<'a> {
    sets: &'a BTreeMap<[u8; 16], SetState>,
}

impl Packets for SetPackets<'_> {
    type Hasher = par2_rs::FileHashState;
    const MAGIC: &'static [u8] = MAGIC;
    const HEADER: usize = HEADER_SIZE;
    // The packet hash covers the packet from its set id on.
    const HASHED_FROM: usize = 32;

    fn ours(&self, header: &[u8]) -> Option<u64> {
        let header = par2_rs::packet::PacketHeader::parse(header, 0).ok()?;
        (self.sets.is_empty() || self.sets.contains_key(header.recovery_set_id.as_bytes()))
            .then_some(header.length)
    }

    fn longest(&self) -> u64 {
        // A recovery packet holds its exponent and one slice.
        let slices = if self.sets.is_empty() {
            UNKNOWN_SLICE.saturating_add(4)
        } else {
            self.sets
                .values()
                .filter_map(|set| set.index.as_ref())
                .map(|index| index.slice_size.saturating_add(4))
                .max()
                .unwrap_or(0)
        };
        (HEADER_SIZE as u64).saturating_add(slices.max(walker::LONGEST_METADATA_BODY))
    }

    fn hasher(&self) -> Self::Hasher {
        par2_rs::FileHashState::new()
    }

    fn update(hasher: &mut Self::Hasher, bytes: &[u8]) {
        hasher.update(bytes);
    }

    fn passes(hasher: Self::Hasher, header: &[u8]) -> bool {
        par2_rs::packet::PacketHeader::parse(header, 0)
            .is_ok_and(|header| hasher.finalize() == header.packet_hash)
    }
}

impl Par2 {
    // One bind's work, its report kept by `bind` around it.
    fn bind_pass(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>) {
        #[cfg(test)]
        if self.scanning {
            if self.shown.bind(namespace, self.id) {
                self.damage_shown();
            }
            let found = self.scan_sets(namespace);
            self.digests += found.digests;
            self.set_checks += found.checks;
            self.bind_all(namespace, renamed, found.sets, found.unnamed);
            self.assert_filed();
            return;
        }
        if self.shown.bind_changed(namespace, self.id) {
            self.damage_shown();
        }
        let pass = self.bind_sets(namespace);
        #[cfg(test)]
        {
            self.digests += pass.digests;
            self.set_checks += pass.checks;
        }
        if pass.whole {
            let bound = self.claims.bindings().clone();
            let unnamed = self.claims.unnamed().list().to_vec();
            self.bind_all(namespace, renamed, bound, unnamed);
        } else {
            self.bind_changed(namespace, renamed, pass);
        }
        #[cfg(test)]
        {
            self.assert_filed();
            self.assert_bound(namespace);
        }
    }
}

impl Recovery for Par2 {
    fn rebuilds(&self) -> bool {
        true
    }

    fn carried_inside(&self) -> bool {
        false
    }

    fn outgrows_recovery(&self) -> bool {
        false
    }

    // The report begins afresh: what this bind moves is kept at each
    // member's first move, and a member bound now that was not as it began
    // is reported with the name it had then.
    fn bind(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>) {
        self.newly.clear();
        self.renamed_from.clear();
        self.covered.start = Some(BTreeMap::new());
        self.bind_pass(namespace, renamed);
        let start = self.covered.start.take().unwrap_or_default();
        #[cfg(test)]
        {
            self.bind_moves = start.len();
        }
        for (member, was) in start {
            if was || !self.covered.by.contains_key(&member) {
                continue;
            }
            let name = self
                .renamed_from
                .remove(&member)
                .or_else(|| Some(namespace.get(member)?.name.clone()));
            self.newly.extend(name.map(|name| (member, name)));
        }
        self.renamed_from.clear();
    }

    fn take_newly(&mut self, out: &mut Vec<(MemberId, String)>) {
        out.append(&mut self.newly);
    }

    // Every file a usable set can rebuild that no member is bound to or may
    // be, once each, in set order: the repair reads an unnamed member's
    // slices before any second copy is announced.
    // A name already listed, by an earlier set or by the caller, is not
    // listed again: one set of what `out` holds answers each file.
    fn absent(&self, out: &mut Vec<(String, u64)>) {
        #[cfg(test)]
        let walked = {
            let mut walked = out.clone();
            self.absent_by_scan(&mut walked);
            walked
        };
        let mut listed: std::collections::HashSet<(&str, u64)> = out
            .iter()
            .map(|(name, length)| (name.as_str(), *length))
            .collect();
        let mut found: Vec<(&str, u64)> = Vec::new();
        for key in self.ordered() {
            let state = &self.sets[key];
            let Some(index) = state.usable() else {
                continue;
            };
            for file in &index.recovery_file_ids {
                let Some(description) = index.file_description(file) else {
                    continue;
                };
                let named = (description.filename.as_str(), description.length);
                if !state.members().contains_key(file) && !state.may_be(description.length) {
                    #[cfg(test)]
                    ABSENT_PROBES.with(|probes| probes.set(probes.get() + 1));
                    if listed.insert(named) {
                        found.push(named);
                    }
                }
            }
        }
        drop(listed);
        out.extend(
            found
                .into_iter()
                .map(|(name, length)| (name.to_owned(), length)),
        );
        #[cfg(test)]
        assert_eq!(*out, walked, "absent");
    }

    fn undescribed(&self) -> bool {
        self.sets.is_empty() || self.sets.values().any(|state| state.usable().is_none())
    }

    fn covers(&self, member: MemberId) -> bool {
        let covered = !self.covered.sets(member).is_empty();
        #[cfg(test)]
        {
            assert_eq!(
                covered,
                self.sets
                    .values()
                    .any(|state| state.bound(member).is_some())
            );
            let mut listed = Vec::new();
            self.covered_members(&mut listed);
            assert_eq!(covered, listed.contains(&member));
        }
        covered
    }

    fn covers_some(&self) -> bool {
        !self.covered.by.is_empty()
    }

    fn covers_any_of(&self, namespace: &Namespace) -> bool {
        self.covered.by.keys().any(|member| {
            #[cfg(test)]
            COVER_PROBES.with(|probes| probes.set(probes.get() + 1));
            namespace.lists(*member)
        })
    }

    fn covered_members(&self, out: &mut Vec<MemberId>) {
        out.extend(self.covered.by.keys().copied());
    }

    fn coverage_version(&self) -> u64 {
        self.covered.changes
    }

    // A take leaves a bind two things: the sets it filed or whose index it
    // changed (`rebound`), and the members a set came to describe, to be
    // said again for it (`unfed`). A member a set gives up moves without
    // one.
    fn bind_due(&self) -> bool {
        !self.rebound.is_empty() || self.moved()
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        let grids = self.list_grids(member);
        if !grids
            .iter()
            .any(|block| self.grid.matches_on(member, *block, segments))
        {
            for key in self.covered.sets(member) {
                let Some(state) = self.sets.get_mut(key) else {
                    continue;
                };
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
                    self.relast.insert(*key);
                }
            }
        }
        self.offer(member, segments, &grids);
        self.grids = grids;
    }

    fn feed_proven(&mut self, member: MemberId, segments: &[Segment]) {
        let grids = self.list_grids(member);
        self.offer(member, segments, &grids);
        self.grids = grids;
    }

    fn moved(&self) -> bool {
        !self.covered.unfed.is_empty()
    }

    // A member a set came to describe, of which something was recorded
    // and nothing on that set's slices: it was said before the set was
    // known to describe it.
    fn unfed(&mut self, out: &mut Vec<MemberId>) {
        let mut sizes = Vec::new();
        for member in std::mem::take(&mut self.covered.unfed) {
            let kept = self.grid.sizes(member);
            sizes.clear();
            self.described_sizes(member, &mut sizes);
            let lacking = sizes.iter().any(|size| !kept.contains(size));
            if !kept.is_empty() && lacking {
                out.push(member);
            }
        }
    }

    // Kept for every member of the namespace, bound or not: a set bound to
    // it later counts what was refused before.
    // Each set binding the member marks the slices the range touches, so it
    // asks for their blocks now rather than after its first repair.
    fn refused(&mut self, member: MemberId, range: ByteRange) {
        let refusal = self.shown.refused(member, range);
        if refusal == Refusal::Damage {
            self.damage_shown();
        }
        if refusal == Refusal::Known {
            return;
        }
        let binding: Vec<([u8; 16], FileId)> = self
            .covered
            .sets(member)
            .iter()
            .filter_map(|key| Some((*key, self.sets.get(key)?.bound(member)?)))
            .collect();
        for (key, file) in &binding {
            self.mark_refused(*key, *file, member, range);
        }
        // A set the refusal leaves short gives the member up now, before the
        // job weighs its rulings, when another set may take it.
        self.settle_short(binding.into_iter().map(|(key, _)| key));
    }

    // A slice is proven by checksums cut at its boundaries; the whole
    // member's CRC folds from any. A member is cut for the sets that
    // describe it, and a member no set binds may be any set's. Each size
    // once, however many sets share it.
    fn block_sizes(&self, member: MemberId, out: &mut Vec<u64>) {
        let from = out.len();
        self.described_sizes(member, out);
        if out.len() == from {
            out.extend(self.slice_sizes());
        }
        out[from..].sort_unstable();
        let mut kept = from;
        for at in from..out.len() {
            if kept == from || out[kept - 1] != out[at] {
                out[kept] = out[at];
                kept += 1;
            }
        }
        out.truncate(kept);
    }

    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        let found = self.ordered().iter().find_map(|key| {
            let state = self.sets.get(key)?;
            state.bound(member)?;
            state.usable()?;
            self.evidence.get(&(*key, member))
        });
        if let Some(evidence) = found {
            out.extend(evidence.vetoed());
        }
    }

    // Per set, ask for the volumes that serve it and are not yet in hand,
    // the fewest blocks first, so the least recovery data is fetched, until
    // what they are named to carry covers its shortfall. A volume whose
    // name gives no count promises nothing toward it, so each such volume
    // is asked for while the shortfall stands. Every
    // other volume the walk has shown carries recovery blocks is deferred:
    // its head told what it is, and the rest waits until a shortfall wants
    // it. Nothing of the deferral is kept: the same walk says it again on
    // every round, after a restart too. A set asked to rule before it has a
    // description, once the namespace shows damage it may cover, or an
    // object asked before it found any set, waits on one volume that may
    // carry a description, and asks for it, then the next once that one is
    // read through without it.
    fn wants(&self, out: &mut Vec<Seek>) {
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
        for &key in self.ordered() {
            let state = &self.sets[&key];
            if let Some(index) = self.awaiting_checksums(&key) {
                if !asked[index] {
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
            // What was refused counts only while the set has not settled,
            // and while the blocks it can reach, known, may cover it: a
            // repair that rebuilt it, or a set the refusals put beyond
            // reach, needs no block for it.
            let (damaged, lost) = self.needs_seen(&key);
            let lost = match lost {
                0 => 0,
                lost if state.concluded().is_none()
                    && self.reach(&key).is_some_and(|reach| lost <= reach) =>
                {
                    lost
                }
                _ => 0,
            };
            let wanted = state.short_of().unwrap_or(0).max(damaged + lost);
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
        // A set that rules short gives its members up, and those that move
        // are ruled again by the sets they moved to, in a further pass. Each
        // move passes a set that gave the member up, or returns it to the
        // first once every set has, so the passes end.
        loop {
            let outcome = self.repair_pass(access, memory_limit);
            if !self.settle_short(self.order.clone()) {
                #[cfg(test)]
                self.assert_filed();
                return outcome;
            }
        }
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.outcome_of_sets()
    }

    // PAR2 ranks above every other format, for now: where a PAR2 set and
    // another set disagree on a member, the PAR2 set's bytes are kept.
    fn precedence(&self) -> u8 {
        1
    }

    fn sets(&self, out: &mut Vec<SetRuling>) {
        self.rulings(out);
    }

    fn unbound(&self, out: &mut Vec<Unbound>) {
        self.unbound_sets(out);
    }

    fn short_sets(&self) -> Option<ShortSets> {
        let mut short = ShortSets::default();
        for &key in self.ordered() {
            let state = &self.sets[&key];
            let ruled = self.refused_short(&key).or(state.concluded());
            match ruled {
                Some(RepairOutcome::Impossible { damaged, reachable }) => {
                    short.sets.push(ShortSet {
                        name: self.set_name(&key),
                        damaged,
                        reachable,
                        members: state.members().values().copied().collect(),
                    });
                }
                Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                    if !state.members().is_empty() =>
                {
                    short.vouched = true;
                }
                _ => {}
            }
        }
        Some(short)
    }
}

impl Par2 {
    // One ruling of every set, in order, folded as `repair` reports it.
    fn repair_pass(&mut self, access: &mut dyn MemberAccess, memory_limit: usize) -> RepairOutcome {
        let damage = self.undescribed_damage();
        let mut outcome = RepairOutcome::Unread;
        for at in 0..self.order.len() {
            let key = self.order[at];
            if self.awaiting_checksums(&key).is_some() {
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
            let mut known = false;
            if state.concluded().is_none() {
                self.relast.insert(key);
            }
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
                // A repair that knows an unnamed member by its slices binds
                // the set to it.
                None => {
                    let before = (!state.unnamed().is_empty()).then(|| state.members().clone());
                    let result = repair_set(state, &self.grid, coming, memory_limit, access);
                    state.last = Some(result);
                    if let Some(before) = before
                        && before != *state.members()
                    {
                        self.covered
                            .rebound(key, before.values(), state.members().values());
                        known = true;
                    }
                    result.outcome
                }
            };
            if known {
                self.disturbed.insert(key);
                self.held(key);
            }
            outcome = combine(outcome, result);
        }
        #[cfg(test)]
        self.assert_filed();
        outcome
    }

    // What the sets ruled, folded from Unread as `repair` folds it, each
    // that came up short waiting while it still does. `None` while a set
    // has not ruled since its bindings changed, or once one no longer waits,
    // so it is tried again. With no set found once a volume of its own is
    // read through, nothing was checked: Unread. Before that a set may
    // still be found in the bytes to come, and it has not ruled.
    fn outcome_of_sets(&self) -> Option<RepairOutcome> {
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
        for &key in self.ordered() {
            if self.awaiting_checksums(&key).is_some() {
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

    fn rulings(&self, out: &mut Vec<SetRuling>) {
        for &key in self.ordered() {
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
                blocks_reachable: (state.recovery.len() as u64).saturating_add(self.counted(&key)),
                damaged_slices: damaged,
                outcome: short.or(last.map(|last| last.outcome)),
                read: short.is_none() && last.is_some_and(|last| last.read),
                name: self.set_name(&key),
            });
        }
    }

    // An object with no set kept none for one of three reasons: it passed
    // its packet or set limits, it read a volume through and found no set in
    // it, or no volume arrived whole to carry an index. Each set it keeps
    // says why it bound nothing.
    fn unbound_sets(&self, out: &mut Vec<Unbound>) {
        if self.sets.is_empty() {
            let reason = match self.refused {
                true => UnboundReason::TooLarge,
                false if self.unread_through() => UnboundReason::Undescribed,
                false => UnboundReason::NoSet,
            };
            out.push(Unbound { id: None, reason });
            return;
        }
        for &key in self.ordered() {
            if let Some(reason) = self.sets[&key].unbound() {
                out.push(Unbound {
                    id: Some(par2_rs::RecoverySetId::from_bytes(key).to_string()),
                    reason,
                });
            }
        }
    }

    // What the set is called: the volume carrying its packets that names
    // the fewest blocks, its index (which names none) before its recovery
    // volumes, as `describing` orders them.
    fn set_name(&self, key: &[u8; 16]) -> Option<String> {
        self.volumes
            .iter()
            .enumerate()
            .filter(|(_, volume)| volume.sets.contains(key) && volume.name.is_some())
            .min_by_key(|(index, volume)| (volume.named_blocks, *index))
            .and_then(|(_, volume)| volume.name.clone())
    }
}

impl Par2 {
    // Bind every set whose binding may change over the claims' `bound` and
    // `unnamed`, renaming by every member and listing every length: the
    // first binding, the one after a restart or after the catalogue
    // changed, and every binding kept to compare with.
    fn bind_all(
        &mut self,
        namespace: &mut Namespace,
        renamed: &mut Vec<MemberId>,
        bound: BTreeMap<[u8; 16], Bindings>,
        unnamed: Vec<(MemberId, Length)>,
    ) {
        // A set binds again where what it binds may change: it was given
        // members, it binds some, or it was marked to. A set given none,
        // binding none and unmarked binds what it did, unless an unmatched
        // member it holds changed or left, or a newly unmatched member could
        // be one of its files. Every set binding a member binds again here.
        if unnamed != self.unmatched {
            for entry in &unnamed {
                let known = self
                    .unmatched
                    .binary_search_by_key(&entry.0, |(member, _)| *member)
                    .is_ok_and(|at| self.unmatched[at] == *entry);
                if !known {
                    self.rebound.extend(holders_of(&self.holders, entry.0));
                    self.catalogue.fitting(entry.1, |(_, key)| {
                        self.rebound.insert(key);
                    });
                }
            }
            for entry in &self.unmatched {
                let kept = unnamed
                    .binary_search_by_key(&entry.0, |(member, _)| *member)
                    .is_ok_and(|at| unnamed[at] == *entry);
                if !kept {
                    self.rebound.extend(holders_of(&self.holders, entry.0));
                }
            }
        }
        #[cfg(test)]
        if self.scanning {
            self.rebound.extend(self.sets.keys().copied());
        }
        let rebound = std::mem::take(&mut self.rebound);
        let mut moved = BTreeSet::new();
        #[cfg(test)]
        let weigh = if self.scanning {
            Weigh::Whole(&unnamed)
        } else {
            Weigh::Kept(self.claims.unnamed())
        };
        #[cfg(not(test))]
        let weigh = Weigh::Kept(self.claims.unnamed());
        // The sets given nothing first, those binding and then those
        // marked, then the sets given members: the order the covered
        // members' changes are counted in.
        let empty = Bindings::new();
        let unchanged = self
            .binding
            .iter()
            .chain(rebound.iter().filter(|key| !self.binding.contains(*key)))
            .filter(|key| !bound.contains_key(*key))
            .map(|key| (*key, &empty));
        for (key, given) in unchanged.chain(bound.iter().map(|(key, given)| (*key, given))) {
            #[cfg(test)]
            {
                self.set_binds += 1;
            }
            self.disturbed.remove(&key);
            rebind(
                &mut self.sets,
                &mut self.covered,
                key,
                given,
                &weigh,
                &mut moved,
            );
        }
        for key in &moved {
            self.held(*key);
        }
        self.unmatched = unnamed;
        // A member a set bound under a name none of its postings answers
        // to the description with, even folded, is recognised: it takes the
        // description's name and length. One bound under its own name keeps
        // it, and takes the description's length as
        // `Namespace::take_described` says, once the set's blocks verify.
        let mut verified = BTreeSet::new();
        for key in &self.binding {
            let state = &self.sets[key];
            let verifies = verifies(state);
            if verifies {
                verified.insert(*key);
            }
            let Some(index) = state.usable() else {
                continue;
            };
            for (file, member) in state.members() {
                #[cfg(test)]
                {
                    self.entries += 1;
                }
                if let Some(description) = index.file_description(file) {
                    rename_by(
                        namespace,
                        description,
                        *member,
                        verifies,
                        renamed,
                        &mut self.renamed_from,
                    );
                }
            }
        }
        self.verified = verified;
        self.relast.clear();
        // A set none of whose bindings changed, and none of whose members'
        // lengths, leaves the lengths as they are: they are its members'.
        for key in &self.binding {
            #[cfg(test)]
            {
                self.entries += self.sets[key].members().len();
            }
            let lengthened = self.sets[key].members().values().any(|member| {
                complete_length(namespace, *member) != self.lengths.get(member).copied()
            });
            if lengthened {
                moved.insert(*key);
            }
        }
        if !moved.is_empty() {
            self.lengths = self
                .binding
                .iter()
                .flat_map(|key| self.sets[key].members().values())
                .filter_map(|member| Some((*member, complete_length(namespace, *member)?)))
                .collect();
            #[cfg(test)]
            {
                self.length_lists += 1;
            }
        }
        self.covered.touched.clear();
        for key in &moved {
            self.fold_set(*key);
        }
        // A set whose binding changed may bind a member it stands short of,
        // as after a restart, which keeps no member given up.
        self.settle_short(moved);
    }

    // Bind only what the claims' `pass` changed since the last binding,
    // as `bind_all` would: the sets whose files it gave other members, the
    // sets the unmatched members may change, and those whose members
    // changed other than by binding; then rename by, and list the lengths
    // of, only the members that changed, the members those sets came to
    // bind or stopped binding, and every member of a set whose members or
    // index changed other than by binding, or whose verdict came to verify
    // or stopped.
    fn bind_changed(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>, pass: Pass) {
        let Pass {
            files,
            unnamed: changes,
            members: dirty,
            ..
        } = pass;
        #[cfg(test)]
        assert_eq!(
            changes.is_empty(),
            self.claims.unnamed().list() == self.unmatched.as_slice(),
            "unnamed changes"
        );
        // An unnamed member that came, went or changed length binds again
        // the sets that could take or lose it: those holding it at a free
        // file, those binding it, and those with a free file it fits now.
        // What a set holds reads only the member's length, the set's free
        // lengths and whether the set binds it.
        for (member, _) in &changes {
            self.rebound.extend(holders_of(&self.holders, *member));
            self.rebound
                .extend(self.covered.sets(*member).iter().copied());
            if let Some(now) = self.claims.unnamed().get(*member) {
                let sets = &self.sets;
                self.catalogue.fitting(now, |(_, key)| {
                    if sets.get(&key).is_some_and(|state| state.fits_free(now)) {
                        self.rebound.insert(key);
                    }
                });
            }
        }
        let rebound = std::mem::take(&mut self.rebound);
        let changed: Vec<MemberId> = changes.iter().map(|(member, _)| *member).collect();
        let claimed = self.claims.bindings();
        let binding = &self.binding;
        let mut group: BTreeSet<[u8; 16]> = files.keys().copied().collect();
        group.extend(rebound.iter().copied());
        group.extend(self.disturbed.iter().copied());
        group.retain(|key| {
            binding.contains(key) || rebound.contains(key) || claimed.contains_key(key)
        });
        // In the order `bind_all` binds them: the order the covered
        // members' changes are counted in.
        let order: Vec<[u8; 16]> = group
            .iter()
            .filter(|key| binding.contains(*key) && !claimed.contains_key(*key))
            .chain(
                group
                    .iter()
                    .filter(|key| !binding.contains(*key) && !claimed.contains_key(*key)),
            )
            .chain(group.iter().filter(|key| claimed.contains_key(*key)))
            .copied()
            .collect();
        let unnamed = self.claims.unnamed();
        let empty = Bindings::new();
        let mut moved = BTreeSet::new();
        for key in &order {
            #[cfg(test)]
            {
                self.set_binds += 1;
            }
            let Some(state) = self.sets.get_mut(key) else {
                continue;
            };
            let given = claimed.get(key).unwrap_or(&empty);
            let change = if self.disturbed.contains(key) {
                state.recompute(given, unnamed)
            } else {
                let files = files.get(key).map_or(&[][..], Vec::as_slice);
                state.bind_changes(given, files, unnamed, &changed)
            };
            if let Some(change) = change {
                self.covered
                    .moved(*key, change.left.iter(), change.entered.iter());
                moved.insert(*key);
            }
        }
        // Every member of a set whose members or index changed other than
        // by binding is renamed by.
        let mut whole = BTreeSet::new();
        for key in &order {
            if self.disturbed.remove(key) {
                whole.insert(*key);
            }
        }
        for key in &moved {
            self.held(*key);
        }
        for (member, _) in &changes {
            let now = self.claims.unnamed().get(*member);
            let at = self
                .unmatched
                .binary_search_by_key(member, |(listed, _)| *listed);
            match (now, at) {
                (Some(length), Ok(at)) => self.unmatched[at].1 = length,
                (Some(length), Err(at)) => self.unmatched.insert(at, (*member, length)),
                (None, Ok(at)) => {
                    self.unmatched.remove(at);
                }
                (None, Err(_)) => {}
            }
        }
        // A set binding members whose verdict came to verify, or stopped,
        // is renamed by whole.
        let relast = std::mem::take(&mut self.relast);
        for key in relast.iter().chain(moved.iter()) {
            let now = self.sets.get(key).is_some_and(verifies);
            let was = if now {
                !self.verified.insert(*key)
            } else {
                self.verified.remove(key)
            };
            if now != was && self.binding.contains(key) {
                whole.insert(*key);
            }
        }
        let mut members = dirty;
        members.extend(std::mem::take(&mut self.covered.touched));
        for key in &whole {
            if let Some(state) = self.sets.get(key) {
                members.extend(state.members().values().copied());
            }
        }
        // Each member is renamed by in every set binding it, in the order
        // `bind_all` renames: by set, then by file.
        let mut entries = BTreeSet::new();
        for member in &members {
            for key in self.covered.sets(*member) {
                for file in self.sets[key].files_of(*member) {
                    entries.insert((*key, file, *member));
                }
            }
        }
        #[cfg(test)]
        {
            self.entries += entries.len();
        }
        let from = renamed.len();
        for (key, file, member) in entries {
            let state = &self.sets[&key];
            let Some(description) = state
                .usable()
                .and_then(|index| index.file_description(&file))
            else {
                continue;
            };
            rename_by(
                namespace,
                description,
                member,
                verifies(state),
                renamed,
                &mut self.renamed_from,
            );
        }
        // The lengths that may have changed: of each member above, and of
        // each renamed.
        members.extend(renamed[from..].iter().copied());
        #[cfg(test)]
        {
            self.entries += members.len();
        }
        let mut lengthened = Vec::new();
        for member in &members {
            let sets = self.covered.sets(*member);
            if !sets.is_empty()
                && complete_length(namespace, *member) != self.lengths.get(member).copied()
            {
                moved.extend(sets.iter().copied());
                lengthened.push(*member);
            }
        }
        if !moved.is_empty() {
            for member in &members {
                let length = complete_length(namespace, *member)
                    .filter(|_| self.covered.by.contains_key(member));
                match length {
                    Some(length) => self.lengths.insert(*member, length),
                    None => self.lengths.remove(member),
                };
            }
            #[cfg(test)]
            {
                self.length_lists += 1;
            }
        }
        // Each set folds the members whose length changed, beside those
        // whose file it changed.
        for key in &moved {
            let state = &self.sets[key];
            let members: Vec<MemberId> = lengthened
                .iter()
                .copied()
                .filter(|member| state.bound(*member).is_some())
                .collect();
            self.fold_members(*key, members);
        }
        self.settle_short(moved);
    }

    // What binding keeps agrees with what binding everything again comes
    // to over the state it left: the lengths listed afresh, each set's
    // members as the claims bind them with those it keeps, its unnamed
    // members and free files, the unmatched members, and the sets whose
    // verdict verifies.
    #[cfg(test)]
    fn assert_bound(&self, namespace: &Namespace) {
        let lengths: BTreeMap<MemberId, u64> = self
            .binding
            .iter()
            .flat_map(|key| self.sets[key].members().values())
            .filter_map(|member| Some((*member, complete_length(namespace, *member)?)))
            .collect();
        assert_eq!(self.lengths, lengths, "lengths");
        let unnamed = self.claims.unnamed();
        assert_eq!(unnamed.list(), self.unmatched.as_slice(), "unmatched");
        let empty = Bindings::new();
        for (key, state) in &self.sets {
            if self.disturbed.contains(key) {
                state.assert_kept(None);
                continue;
            }
            state.assert_kept(Some(unnamed.list()));
            let given = self.claims.bindings().get(key).unwrap_or(&empty);
            for (file, member) in given {
                assert_eq!(state.members().get(file), Some(member), "given");
            }
            for (file, member) in state.members() {
                if !given.contains_key(file) {
                    assert!(unnamed.get(*member).is_some(), "kept {member:?}");
                }
            }
        }
        for key in self
            .binding
            .iter()
            .filter(|key| !self.relast.contains(*key))
        {
            assert_eq!(
                verifies(&self.sets[key]),
                self.verified.contains(key),
                "verified"
            );
        }
    }

    // How many set-member entries binding has visited.
    #[cfg(test)]
    fn entries(&self) -> usize {
        self.entries + self.sets.values().map(|state| state.visited).sum::<usize>()
    }
}

// Whether the set's last verdict verifies its blocks.
fn verifies(state: &SetState) -> bool {
    matches!(
        state.concluded(),
        Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
    )
}

// A member a set binds to a file it describes, named as the description
// says when none of its postings answers to it. `before` keeps the name a
// member had before the bind first named it.
fn rename_by(
    namespace: &mut Namespace,
    description: &par2_rs::FileDescription,
    member: MemberId,
    verified: bool,
    renamed: &mut Vec<MemberId>,
    before: &mut BTreeMap<MemberId, String>,
) {
    let answers = namespace
        .postings(member)
        .any(|record| record.name.eq_ignore_ascii_case(&description.filename));
    if !answers {
        if let Some(record) = namespace.get(member) {
            before.entry(member).or_insert_with(|| record.name.clone());
        }
        if namespace
            .recognise(member, &description.filename, description.length)
            .is_some()
        {
            renamed.push(member);
        }
        return;
    }
    if namespace.take_described(member, description.length, verified) {
        renamed.push(member);
    }
}

// Bind the set to what binding gave it: one whose binding changed has
// moved, and its members are counted as covered in place of those it had.
fn rebind(
    sets: &mut BTreeMap<[u8; 16], SetState>,
    covered: &mut Cover,
    key: [u8; 16],
    given: &Bindings,
    weigh: &Weigh<'_>,
    moved: &mut BTreeSet<[u8; 16]>,
) {
    let Some(state) = sets.get_mut(&key) else {
        return;
    };
    match weigh {
        Weigh::Kept(unnamed) => {
            if let Some(change) = state.recompute(given, unnamed) {
                covered.moved(key, change.left.iter(), change.entered.iter());
                moved.insert(key);
            }
        }
        #[cfg(test)]
        Weigh::Whole(unnamed) => {
            if let Some(was) = state.bind_whole(given.clone(), unnamed) {
                covered.rebound(key, was.values(), state.members().values());
                moved.insert(key);
            }
        }
    }
}

// The sets holding `member` at a free file.
fn holders_of(
    holders: &BTreeSet<(MemberId, [u8; 16])>,
    member: MemberId,
) -> impl Iterator<Item = [u8; 16]> + '_ {
    holders
        .range((member, [0; 16])..=(member, [u8::MAX; 16]))
        .map(|(_, key)| *key)
}

// The unnamed members a set is bound over: as the claims keep them, or,
// for the binding kept to compare with, as a list weighed whole.
enum Weigh<'a> {
    Kept(&'a bind::Unnamed),
    #[cfg(test)]
    Whole(&'a [(MemberId, Length)]),
}

// Which sets bind each member, and how many times the members bound
// changed: a member that comes to be bound or stops being bound changes
// what the set covers.
#[derive(Default)]
struct Cover {
    // Each bound member's sets, in key order.
    by: BTreeMap<MemberId, Vec<[u8; 16]>>,
    // Members a set came to bind or to describe since the namespace last
    // asked. What arrived of one before was recorded for the sets that
    // described it then, so the namespace says it again for this one.
    unfed: BTreeSet<MemberId>,
    changes: u64,
    // Members a set came to bind or stopped binding since the last
    // binding took them: binding renames by them, and lists their lengths.
    touched: BTreeSet<MemberId>,
    // While a bind runs: each member it moved, with whether it was bound
    // as the bind began, kept at the member's first move.
    start: Option<BTreeMap<MemberId, bool>>,
}

impl Cover {
    fn sets(&self, member: MemberId) -> &[[u8; 16]] {
        self.by.get(&member).map_or(&[], Vec::as_slice)
    }

    // Keeps whether `member` was bound as the running bind began, at its
    // first move.
    fn keep_start(&mut self, member: MemberId) {
        let bound = self.by.contains_key(&member);
        if let Some(start) = &mut self.start {
            start.entry(member).or_insert(bound);
        }
    }

    // Every set gives up its members at once.
    fn clear(&mut self) {
        if let Some(start) = &mut self.start {
            for member in self.by.keys() {
                start.entry(*member).or_insert(true);
            }
        }
        if !self.by.is_empty() {
            self.changes += 1;
        }
        self.by.clear();
        self.unfed.clear();
        self.touched.clear();
    }

    // The set `key` bound the members `was` and binds the members `now`.
    fn rebound<'a>(
        &mut self,
        key: [u8; 16],
        was: impl Iterator<Item = &'a MemberId>,
        now: impl Iterator<Item = &'a MemberId>,
    ) {
        let was: BTreeSet<MemberId> = was.copied().collect();
        let now: BTreeSet<MemberId> = now.copied().collect();
        self.moved(key, was.difference(&now), now.difference(&was));
    }

    // The set `key` stopped binding the members `left` and came to bind
    // the members `entered`.
    fn moved<'a>(
        &mut self,
        key: [u8; 16],
        left: impl Iterator<Item = &'a MemberId>,
        entered: impl Iterator<Item = &'a MemberId>,
    ) {
        for member in left {
            self.keep_start(*member);
            self.touched.insert(*member);
            if let Some(sets) = self.by.get_mut(member) {
                sets.retain(|set| *set != key);
                if sets.is_empty() {
                    self.by.remove(member);
                    self.changes += 1;
                }
            }
        }
        for member in entered {
            self.keep_start(*member);
            self.touched.insert(*member);
            if !self.by.contains_key(member) {
                self.changes += 1;
            }
            let sets = self.by.entry(*member).or_default();
            if let Err(at) = sets.binary_search(&key) {
                self.unfed.insert(*member);
                sets.insert(at, key);
            }
        }
    }
}

// A member's length, once all of it has arrived.
fn complete_length(namespace: &Namespace, member: MemberId) -> Option<u64> {
    let record = namespace.get(member)?;
    record.length.filter(|_| record.is_complete())
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
