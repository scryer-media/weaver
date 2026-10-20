// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR3 object. Same shape as the PAR2 one: built over the first .par3
// member, it claims the others as further volumes, and the namespace keeps
// every volume's bytes in the member's own file.
//
// It declines the segment feed. A PAR3 block is checked by a BLAKE3
// fingerprint, not a CRC32, so the segment checksums fold into nothing it
// can compare; it reads members back to verify them instead. It keeps the
// front of each volume to learn early which members it describes, and reads
// every arrived run of each volume back when it repairs. Only packets that
// lie wholly in a volume's readable ranges count: one a hole cuts is
// dropped, and the scan carries on past it.
//
// A member binds to a described file by its path or bare name, or, when no
// described name answers to it, by its bytes: the repair reads it back and
// identifies it, and the next bind gives it the described name. A described
// file no member is is announced absent, and the repair writes it whole.

pub(crate) mod facts;
mod identity;
mod rebuild;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use bytes::Bytes;
use par3_rs::layout::{BlockLayout, FileLayout};
use par3_rs::runtime::ExecutionOptions;
use par3_rs::{MAGIC, Packet, Par3Set, ScanLimits};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome, SetRuling};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Kind, MemberId, Namespace, Object, ObjectId,
    Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use crate::par2::combine;

// How much of each volume's front is kept to read the index from. An index
// file is metadata only; a recovery volume repeats it ahead of its blocks.
const FRONT_CAP: usize = 1024 * 1024;
// What one job may carry.
const MAX_CARRIERS: usize = 4096;
const MAX_SETS: usize = 256;
// What one repair's decoder may hold: half the allowance a job has when the
// host's memory is unknown.
const REPAIR_MEMORY_LIMIT: u64 = 128 << 20;

// A set's ruling and the members its files were bound to when it ruled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Settled {
    members: Vec<Option<MemberId>>,
    slice_size: u64,
    blocks_reachable: u64,
    damaged_slices: u64,
    outcome: RepairOutcome,
}

impl Settled {
    // A PAR3 ruling reads every bound member back.
    fn ruling(&self, id: &str) -> SetRuling {
        SetRuling {
            id: id.to_owned(),
            slice_size: self.slice_size,
            blocks_reachable: self.blocks_reachable,
            damaged_slices: self.damaged_slices,
            outcome: Some(self.outcome),
            read: true,
        }
    }
}

// What the last repair came to: the verdict it reached, or that it came up
// short with so many volumes closed. A repair spends recovery by reading the
// volumes back, so what it waits for is in hand once another closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Ruling {
    Reached(RepairOutcome),
    Short { closed: usize },
}

#[derive(Debug, Default)]
struct Volume {
    front: Vec<u8>,
    arrived: Coverage,
    closed: bool,
}

// A member that may be a described file no member carries: no described
// name answers to it, and its length is that file's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Unnamed {
    length: u64,
    complete: bool,
}

pub struct Par3 {
    id: ObjectId,
    volumes: Vec<Volume>,
    // Described paths and the member each is bound to.
    bound: BTreeMap<String, MemberId>,
    // Members that may be a free described file. Those complete are what a
    // repair reads back to identify.
    unnamed: BTreeMap<MemberId, Unnamed>,
    // Every described path, and whether a member carries its name: only a
    // path none carries binds by content.
    described: BTreeMap<String, bool>,
    // The described path a readback identified each unnamed member as.
    identified: BTreeMap<MemberId, String>,
    // Damaged blocks the last repair found while it waited.
    needed: u64,
    // What the last repair came to, for the members bound then.
    last: Option<Ruling>,
    // Each set as the last repair ruled it.
    ruled: Vec<SetRuling>,
    // Each set a repair settled, by its id: what it ruled and the members
    // its files were bound to then. The ruling stands while they are; only
    // a set that waited or could not rebuild is tried again.
    settled: BTreeMap<String, Settled>,
    memory_limit: u64,
}

impl Par3 {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: Vec::new(),
            bound: BTreeMap::new(),
            unnamed: BTreeMap::new(),
            described: BTreeMap::new(),
            identified: BTreeMap::new(),
            needed: 0,
            last: None,
            ruled: Vec::new(),
            settled: BTreeMap::new(),
            memory_limit: REPAIR_MEMORY_LIMIT,
        }
    }

    fn volume(&mut self, index: VolumeIndex) -> &mut Volume {
        let at = index.0 as usize;
        if self.volumes.len() <= at {
            self.volumes.resize_with(at + 1, Volume::default);
        }
        &mut self.volumes[at]
    }

    // The sets as far as the readable spans of the kept fronts describe them.
    fn front_sets(&self) -> Vec<Par3Set> {
        let mut packets = Vec::new();
        for volume in &self.volumes {
            for span in readable(&volume.arrived) {
                let end = span.end.min(volume.front.len() as u64);
                if let Some(bytes) = volume.front.get(span.start as usize..end as usize) {
                    scan(bytes, &mut packets);
                }
            }
        }
        sets(packets)
    }

    fn coming(&self) -> bool {
        self.volumes.iter().any(|volume| !volume.closed)
    }

    fn closed(&self) -> usize {
        self.volumes.iter().filter(|volume| volume.closed).count()
    }

    fn free(&self, path: &str) -> bool {
        self.described.get(path) == Some(&false)
    }

    // The free paths exactly one unnamed member was identified as.
    fn by_content(&self) -> BTreeMap<String, MemberId> {
        let mut claims: BTreeMap<&str, Vec<MemberId>> = BTreeMap::new();
        for (member, path) in &self.identified {
            let complete = self
                .unnamed
                .get(member)
                .is_some_and(|member| member.complete);
            if complete && self.free(path) {
                claims.entry(path).or_default().push(*member);
            }
        }
        claims
            .into_iter()
            .filter_map(|(path, members)| match members.as_slice() {
                [member] => Some((path.to_owned(), *member)),
                _ => None,
            })
            .collect()
    }

    // Read back every unnamed member not yet identified and bind those the
    // sets now name by content, so this repair verifies them with the rest.
    fn identify(&mut self, sets: &[Par3Set], access: &mut dyn MemberAccess) {
        if self.unnamed.is_empty() {
            return;
        }
        let layouts: Vec<BlockLayout> = sets
            .iter()
            .filter_map(|set| BlockLayout::new(set, &ExecutionOptions::default()).ok())
            .collect();
        let files: Vec<&FileLayout> = layouts.iter().flat_map(BlockLayout::files).collect();
        let mut read = 0;
        for (&member, unnamed) in &self.unnamed {
            if !unnamed.complete || self.identified.contains_key(&member) {
                continue;
            }
            let free = |path: &str| self.free(path);
            let len = unnamed.length;
            if let Some(path) = identity::identify(&files, free, access, member, len, &mut read) {
                self.identified.insert(member, path.to_owned());
            }
        }
        let found = self.by_content();
        self.bound.extend(found);
    }

    // A free file of the set that a member still arriving may turn out to
    // be is not missing yet: the set waits for that member, and asks for no
    // recovery on its account.
    fn identifying(&self, set: &Par3Set) -> bool {
        set.files().iter().any(|file| {
            !self.bound.contains_key(file.path())
                && self.free(file.path())
                && self
                    .unnamed
                    .values()
                    .any(|member| !member.complete && member.length == file.size())
        })
    }
}

// A volume's readable spans: what arrived, runs that abut joined, so a
// packet is cut only where a hole is.
fn readable(arrived: &Coverage) -> Vec<ByteRange> {
    let mut spans: Vec<ByteRange> = Vec::new();
    for run in arrived.runs() {
        match spans.last_mut() {
            Some(last) if last.abuts(run.range) => last.end = run.range.end,
            _ => spans.push(run.range),
        }
    }
    spans
}

// The packets of one readable span. Each span is scanned on its own: a
// packet a hole cuts is dropped and the scan picks up past the hole, and a
// span that fails to scan contributes nothing.
fn scan(bytes: &[u8], into: &mut Vec<Packet>) {
    if let Ok(found) = par3_rs::scan_packets_with_limits(bytes, &ScanLimits::default()) {
        into.extend(found.into_iter().map(|(_, packet)| packet));
    }
}

// Every set the packets describe; a job of more sets than the limit has none.
fn sets(packets: Vec<Packet>) -> Vec<Par3Set> {
    let sets = Par3Set::from_packets(packets).unwrap_or_default();
    if sets.len() > MAX_SETS {
        return Vec::new();
    }
    sets
}

impl Object for Par3 {
    fn kind(&self) -> Kind {
        Kind::Par3
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
        let volume = self.volume(delivery.volume);
        volume.arrived.write(delivery.range(), delivery.provenance);
        let Ok(offset) = usize::try_from(delivery.offset) else {
            return;
        };
        // Bytes past a hole are kept past zeroes, which no packet hash
        // passes.
        if offset >= FRONT_CAP {
            return;
        }
        let end = offset.saturating_add(delivery.bytes.len()).min(FRONT_CAP);
        if end > volume.front.len() {
            volume.front.resize(end, 0);
        }
        volume.front[offset..end].copy_from_slice(&delivery.bytes[..end - offset]);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.volume(volume).closed = true;
    }

    // A loss is a hole in what arrived, which no packet in it passes; the
    // volume still closes once the rest is in.
    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    fn wants(&mut self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Every answer means wait: a lost article of the volume reaches
    // `volume_lost` at its exact range, and the rest of it still comes.
    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

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

    // Every PAR3 file is a volume of the one object, numbered as it comes,
    // up to the carrier limit.
    fn claim(
        &mut self,
        _name: &str,
        first_bytes: &[u8],
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        (first_bytes.get(..MAGIC.len()) == Some(MAGIC.as_slice())
            && self.volumes.len() < MAX_CARRIERS)
            .then(|| VolumeIndex(self.volumes.len() as u32))
    }

    fn bound(&mut self, volume: VolumeIndex, _name: &str, _front: &[u8], _length: Option<u64>) {
        self.volume(volume);
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Par3 {
    // By path or bare name, where only a unique match binds; or by content,
    // for a member no described name answers to, which takes the described
    // name.
    fn bind(&mut self, namespace: &mut Namespace, renamed: &mut Vec<MemberId>) {
        let sets = self.front_sets();
        let files = || sets.iter().flat_map(Par3Set::files);
        let answers =
            |name: &str, file: &par3_rs::Par3File| name == file.path() || name == file.name();
        let others = || {
            namespace
                .members()
                .filter(|member| member.bound_to != Some(self.id))
        };
        let mut bound = BTreeMap::new();
        for file in files() {
            let mut matches = others().filter(|member| answers(&member.name, file));
            if let (Some(member), None) = (matches.next(), matches.next()) {
                bound.insert(file.path().to_owned(), member.id);
            }
        }
        let described: BTreeMap<String, bool> = files()
            .map(|file| {
                let carried = others().any(|member| answers(&member.name, file));
                (file.path().to_owned(), carried)
            })
            .collect();
        let free_size = |length: u64| {
            files().any(|file| file.size() == length && described.get(file.path()) == Some(&false))
        };
        let unnamed: BTreeMap<MemberId, Unnamed> = others()
            .filter(|member| !files().any(|file| answers(&member.name, file)))
            .filter_map(|member| {
                let length = member.length.filter(|length| free_size(*length))?;
                let complete = member.is_complete();
                Some((member.id, Unnamed { length, complete }))
            })
            .collect();
        // A verdict stands on what the object could bind when it ruled: the
        // files described, the members bound, and the members it may yet
        // identify once it is complete. A rename only moves a member from
        // the last to the second, so a member completing is what makes it
        // stale.
        let learned = !described.keys().eq(self.described.keys())
            || unnamed.iter().any(|(member, now)| {
                now.complete && self.unnamed.get(member).is_none_or(|was| !was.complete)
            });
        self.unnamed = unnamed;
        self.described = described;
        let unnamed = &self.unnamed;
        self.identified
            .retain(|member, _| unnamed.contains_key(member));
        for (path, member) in self.by_content() {
            let length = self.unnamed[&member].length;
            if namespace.recognise(member, &path, length).is_some() {
                renamed.push(member);
            }
            bound.insert(path, member);
        }
        if learned || bound != self.bound {
            self.last = None;
        }
        self.bound = bound;
    }

    // Every described file no member carries by name and none may yet turn
    // out to be by content, by path and size, once each. A member of the
    // file's size that no described name answers to may be it until a
    // readback says what it is.
    fn absent(&self, out: &mut Vec<(String, u64)>) {
        for set in self.front_sets() {
            for file in set.files() {
                let path = file.path();
                let maybe = self.unnamed.iter().any(|(member, unnamed)| {
                    unnamed.length == file.size()
                        && self.identified.get(member).is_none_or(|was| was == path)
                });
                let named = (path.to_owned(), file.size());
                if self.free(path)
                    && !self.bound.contains_key(path)
                    && !maybe
                    && !out.contains(&named)
                {
                    out.push(named);
                }
            }
        }
    }

    fn covers(&self, member: MemberId) -> bool {
        self.bound.values().any(|bound| *bound == member)
    }

    fn feed(&mut self, _member: MemberId, _segments: &[Segment]) {}

    // Nothing is known before a readback.
    fn verdicts(&self, _member: MemberId, _out: &mut Vec<SliceVerdict>) {}

    fn wants(&mut self, out: &mut Vec<Seek>) {
        if self.needed == 0 {
            return;
        }
        for (index, volume) in self.volumes.iter().enumerate() {
            if volume.closed {
                continue;
            }
            let start = volume.arrived.runs().next().map_or(0, |run| {
                if run.range.start == 0 {
                    run.range.end
                } else {
                    0
                }
            });
            out.push(Seek::recovery(
                VolumeIndex(index as u32),
                ByteRange::new(start, u64::MAX),
                Weight::FIRST,
            ));
        }
    }

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        // Every readable span of every volume, read back and scanned one at
        // a time; a span that does not read back whole is left out.
        let mut packets = Vec::new();
        let mut bytes = Vec::new();
        for (index, volume) in self.volumes.iter().enumerate() {
            for span in readable(&volume.arrived) {
                bytes.clear();
                if access.read_volume(VolumeIndex(index as u32), span, &mut bytes)
                    && bytes.len() as u64 == span.len()
                {
                    scan(&bytes, &mut packets);
                }
            }
        }
        let sets = sets(packets);
        self.identify(&sets, access);
        let coming = self.coming();
        let mut outcome = RepairOutcome::Clean;
        let mut waiting = 0;
        self.ruled.clear();
        for set in sets {
            // The digest of the set's root packet, which its recovery packets
            // name it by.
            let id: String = set
                .root_hash()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            let members: Vec<Option<MemberId>> = set
                .files()
                .iter()
                .map(|file| self.bound.get(file.path()).copied())
                .collect();
            if let Some(settled) = self.settled.get(&id).filter(|was| was.members == members) {
                self.ruled.push(settled.ruling(&id));
                outcome = combine(outcome, settled.outcome);
                continue;
            }
            let result = if self.identifying(&set) {
                rebuild::SetRepair {
                    outcome: RepairOutcome::Waiting,
                    lost: 0,
                }
            } else {
                rebuild::repair_set(&set, &self.bound, coming, self.memory_limit, access)
            };
            if result.outcome == RepairOutcome::Waiting {
                waiting += result.lost;
            }
            let settled = Settled {
                members,
                slice_size: set.block_size(),
                blocks_reachable: rebuild::usable_recovery(&set).len() as u64,
                damaged_slices: result.lost,
                outcome: result.outcome,
            };
            self.ruled.push(settled.ruling(&id));
            if matches!(
                result.outcome,
                RepairOutcome::Clean | RepairOutcome::Repaired { .. }
            ) {
                self.settled.insert(id, settled);
            }
            outcome = combine(outcome, result.outcome);
        }
        self.needed = waiting;
        self.last = Some(match outcome {
            RepairOutcome::Waiting => Ruling::Short {
                closed: self.closed(),
            },
            reached => Ruling::Reached(reached),
        });
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        match self.last? {
            Ruling::Reached(outcome) => Some(outcome),
            Ruling::Short { closed } => (self.closed() == closed).then_some(RepairOutcome::Waiting),
        }
    }

    fn sets(&self, out: &mut Vec<SetRuling>) {
        // A bind since the last repair voids its rulings with its outcome,
        // as a wait that is over does.
        let current = self.outcome().is_some();
        out.extend(self.ruled.iter().map(|ruled| SetRuling {
            outcome: ruled.outcome.filter(|_| current),
            ..ruled.clone()
        }));
    }
}
