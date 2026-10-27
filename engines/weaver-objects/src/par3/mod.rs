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
use par3_rs::{HEADER_SIZE, MAGIC, Packet, PacketHeader, PacketType, Par3Set, ScanLimits};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome, SetRuling};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Kind, Located, Member, MemberId, Namespace,
    Object, ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use crate::par2::combine;
use crate::recovery_volume::{self, Refusal, Shown};

// How much of each volume's front is kept to read the index from. An index
// file is metadata only; a recovery volume repeats it ahead of its blocks.
const FRONT_CAP: usize = 1024 * 1024;
// What one job may carry.
const MAX_CARRIERS: usize = 4096;
const MAX_SETS: usize = 256;
// What one repair's decoder may hold when the owner grants it no memory:
// half the allowance a job has when the host's memory is unknown.
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
// volumes back, so what it waits for is in hand once another closes. One
// that read no set whole found what describes the files in bytes not yet
// read: it waits, as a short one does, on the volume that may describe them
// while damage shows, and otherwise checked nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Ruling {
    Reached(RepairOutcome),
    Short { closed: usize },
    Undescribed { closed: usize, waiting: bool },
}

#[derive(Debug, Default)]
struct Volume {
    front: Vec<u8>,
    arrived: Coverage,
    closed: bool,
    // The recovery blocks the volume's name says it carries.
    named_blocks: Option<u64>,
}

impl Volume {
    // Whether the volume shows it carries recovery blocks: its packets,
    // header by header from its start through what arrived of its front,
    // reach a recovery packet, or its name says it carries some and its
    // front holds a packet. A recovery volume repeats the index ahead of
    // its blocks, so its first article may hold only index packets.
    fn shows_recovery(&self) -> bool {
        let Some(first) = readable(&self.arrived).into_iter().next() else {
            return false;
        };
        if first.start != 0 {
            return false;
        }
        let end = first.end.min(self.front.len() as u64);
        let mut at = 0u64;
        while at.saturating_add(HEADER_SIZE as u64) <= end {
            let Ok(header) = PacketHeader::parse(&self.front[at as usize..end as usize], at) else {
                return false;
            };
            if matches!(
                header.packet_type,
                PacketType::RecoveryData | PacketType::RecoveryExternalData
            ) {
                return true;
            }
            at = at.saturating_add(header.length);
        }
        at > 0 && self.named_blocks.unwrap_or(0) > 0
    }
}

// A member that may be a described file no member carries: no described
// name answers to it, and its length fits that file's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Unnamed {
    // Exact once any byte of it arrived checked; otherwise only the bound
    // its post's encoded size sets, as a PAR2 member's is.
    length: u64,
    intact: bool,
    complete: bool,
}

impl Unnamed {
    fn of(member: &Member) -> Option<Self> {
        Some(Self {
            length: member.length?,
            intact: member
                .coverage
                .runs()
                .any(|run| run.provenance.is_verified()),
            complete: member.is_complete(),
        })
    }

    // Whether the member could be a described file of `long` bytes.
    fn fits(self, long: u64) -> bool {
        if self.intact {
            self.length == long
        } else {
            long <= self.length
        }
    }
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
    // The repair's decoder allowance when the owner grants it none.
    memory_limit: u64,
    // The namespace's damage as the object was shown it, and the whole
    // blocks of bound files its refusals touch, counted again whenever
    // either changes.
    shown: Shown,
    refused_blocks: u64,
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
            shown: Shown::default(),
            refused_blocks: 0,
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

    // The whole blocks of the bound files a refused range touches. A
    // file's last, partial block may be packed with another file's, so it
    // is left out: the count never runs ahead of the blocks really lost.
    fn count_refused(&mut self, sets: &[Par3Set]) {
        let mut blocks = 0u64;
        for set in sets {
            let block = set.block_size();
            if block == 0 {
                continue;
            }
            for file in set.files() {
                let Some(ranges) = self
                    .bound
                    .get(file.path())
                    .and_then(|member| self.shown.refusals.get(member))
                else {
                    continue;
                };
                let whole = file.size() / block;
                let touched: std::collections::BTreeSet<u64> = ranges
                    .iter()
                    .filter_map(|range| range.intersect(ByteRange::new(0, whole * block)))
                    .flat_map(|range| range.start / block..=(range.end - 1) / block)
                    .collect();
                blocks += touched.len() as u64;
            }
        }
        self.refused_blocks = blocks;
    }

    // The blocks the volumes can still bring, by what their names say. A
    // volume that names none carries none once it is read through without
    // showing a recovery packet; before that it says nothing, and `None`.
    // A loss inside a volume is not counted against it, so the reach never
    // falls short of the blocks really there.
    fn reach(&self) -> Option<u64> {
        self.volumes
            .iter()
            .map(|volume| match volume.named_blocks {
                Some(blocks) => Some(blocks),
                None if volume.closed && !volume.shows_recovery() => Some(0),
                None => None,
            })
            .sum()
    }

    // The object's ruling on what was refused: impossible once the refused
    // blocks outrun what its volumes can bring.
    fn refused_short(&self) -> Option<RepairOutcome> {
        let damaged = self.refused_blocks;
        if damaged == 0 {
            return None;
        }
        let reachable = self.reach()?;
        (damaged > reachable).then_some(RepairOutcome::Impossible { damaged, reachable })
    }

    // Whether the namespace shows damage the sets not yet read may cover: a
    // member, none of the object's own volumes, held damaged or refused,
    // that no described file is bound to. Only then is a volume read for
    // the descriptions: on a whole post the object checks nothing and
    // costs nothing.
    fn undescribed_damage(&self) -> bool {
        self.shown.undescribed(|member| self.covers(member))
    }

    // The volume to read for the descriptions while no set was read whole
    // and damage shows. `None` otherwise.
    fn describing(&self) -> Option<usize> {
        let undescribed = matches!(self.last, Some(Ruling::Undescribed { .. }));
        undescribed.then(|| self.describer()).flatten()
    }

    // The volume that may describe the sets while damage shows: the first
    // still open, an index before any volume and then the fewest blocks
    // first.
    fn describer(&self) -> Option<usize> {
        if !self.undescribed_damage() {
            return None;
        }
        let mut order: Vec<usize> = (0..self.volumes.len()).collect();
        order.sort_by_key(|index| (self.volumes[*index].named_blocks, *index));
        order.into_iter().find(|index| !self.volumes[*index].closed)
    }

    // Damage the namespace shows for the first time makes an object that
    // read no set, and ruled it checked nothing, due again: on what it saw
    // then, no volume was worth reading for the sets; now one is.
    // An impossible verdict stands: damage only grows, so what the object
    // could not rebuild it still cannot.
    fn damage_shown(&mut self) {
        if let Some(Ruling::Undescribed { waiting: false, .. }) = self.last {
            self.last = None;
        }
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
            if !unnamed.complete || !unnamed.intact || self.identified.contains_key(&member) {
                continue;
            }
            let free = |path: &str| self.free(path);
            let len = unnamed.length;
            if let Some(path) = identity::identify(&files, free, access, member, len, &mut read) {
                self.identified.insert(member, path.to_owned());
            }
        }
        // A member with no intact byte has nothing to say what it is, so it
        // is what is left, as a PAR2 member no slice named is: longest file
        // first, each free file no member is takes the member it fits most
        // tightly, once the files those members could be are no more than
        // they are. The repair makes every byte of it over.
        let mut free: Vec<(String, u64)> = files
            .iter()
            .filter(|file| {
                file.len != 0
                    && self.free(&file.path)
                    && !self.bound.contains_key(&file.path)
                    && !self.identified.values().any(|path| *path == file.path)
            })
            .map(|file| (file.path.clone(), file.len))
            .collect();
        free.sort_by_key(|(_, long)| std::cmp::Reverse(*long));
        free.dedup();
        let mut blank: Vec<(MemberId, Unnamed)> = self
            .unnamed
            .iter()
            .filter(|(member, unnamed)| {
                unnamed.complete && !unnamed.intact && !self.identified.contains_key(member)
            })
            .map(|(member, unnamed)| (*member, *unnamed))
            .collect();
        while let Some((at, member)) = free.iter().enumerate().find_map(|(at, (_, long))| {
            let fitting: Vec<usize> = (0..blank.len())
                .filter(|candidate| blank[*candidate].1.fits(*long))
                .collect();
            let could_be = free
                .iter()
                .filter(|(_, other)| fitting.iter().any(|c| blank[*c].1.fits(*other)))
                .count();
            let tightest = fitting
                .iter()
                .copied()
                .min_by_key(|candidate| blank[*candidate].1.length)
                .filter(|_| could_be <= fitting.len())?;
            Some((at, tightest))
        }) {
            let (path, _) = free.remove(at);
            self.identified.insert(blank.swap_remove(member).0, path);
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
                    .any(|member| !member.complete && member.fits(file.size()))
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

    fn wants(&self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    // Every answer means wait: a lost article of the volume reaches
    // `volume_lost` at its exact range, and the rest of it still comes.
    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // What arrived there is to come again, and the blocks it carried; a
    // volume closed over them is open until they do.
    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            found.arrived.erase(range);
            found.closed = false;
        }
    }

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
            .then_some(VolumeIndex(self.volumes.len() as u32))
    }

    // An impossible verdict counted the blocks of the volumes bound then,
    // and an unread one the packets they could bring; a volume bound since
    // may bring more of either, so the object is tried again. Without this
    // the verdict would depend on whether a volume's front decided before
    // or after the damage arrived.
    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        self.volume(volume).named_blocks = recovery_volume::named_blocks(name, ".par3");
        if matches!(
            self.last,
            Some(
                Ruling::Reached(RepairOutcome::Impossible { .. } | RepairOutcome::Unread)
                    | Ruling::Undescribed { .. }
            )
        ) {
            self.last = None;
        }
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
        if self.shown.bind(namespace, self.id) {
            self.damage_shown();
        }
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
        let fits_free = |unnamed: Unnamed| {
            files()
                .any(|file| unnamed.fits(file.size()) && described.get(file.path()) == Some(&false))
        };
        let unnamed: BTreeMap<MemberId, Unnamed> = others()
            .filter(|member| !files().any(|file| answers(&member.name, file)))
            .filter_map(|member| Some((member.id, Unnamed::of(member).filter(|u| fits_free(*u))?)))
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
            let Some(length) = files()
                .find(|file| file.path() == path)
                .map(|file| file.size())
            else {
                continue;
            };
            if namespace.recognise(member, &path, length).is_some() {
                renamed.push(member);
            }
            bound.insert(path, member);
        }
        if learned || bound != self.bound {
            self.last = None;
        }
        self.bound = bound;
        self.count_refused(&sets);
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
                    unnamed.fits(file.size())
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

    // Kept for every member of the namespace, bound or not: a bind later
    // counts what was refused before.
    fn refused(&mut self, member: MemberId, range: ByteRange) {
        match self.shown.refused(member, range) {
            Refusal::Known => return,
            Refusal::Own => {}
            Refusal::Damage => self.damage_shown(),
        }
        let sets = self.front_sets();
        self.count_refused(&sets);
    }

    // Nothing is known before a readback.
    fn verdicts(&self, _member: MemberId, _out: &mut Vec<SliceVerdict>) {}

    // While the last repair waits on damaged blocks, every open volume is
    // wanted now. While it read no set whole and damage shows, the one
    // volume that may describe them is. Otherwise every volume whose head
    // shows it carries recovery is deferred: the rest waits until a repair
    // is short. Nothing of the deferral is kept: the same fronts say it
    // again on every round.
    fn wants(&self, out: &mut Vec<Seek>) {
        let describing = self.describing();
        for (index, volume) in self.volumes.iter().enumerate() {
            if volume.closed {
                continue;
            }
            if describing == Some(index) {
                recovery_volume::gaps(&volume.arrived, |range| {
                    out.push(Seek::recovery(
                        VolumeIndex(index as u32),
                        range,
                        Weight::FIRST,
                    ))
                });
                continue;
            }
            if self.needed == 0 {
                if volume.shows_recovery() {
                    recovery_volume::gaps(&volume.arrived, |range| {
                        out.push(Seek::deferred(VolumeIndex(index as u32), range))
                    });
                }
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

    fn repair(
        &mut self,
        access: &mut dyn MemberAccess,
        budget: weaver_graph::Budget,
    ) -> RepairOutcome {
        if let Some(short) = self.refused_short() {
            return short;
        }
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
        // With no set read whole, one waits on a volume that may describe
        // it while damage shows; otherwise nothing was checked.
        if sets.is_empty() {
            self.needed = 0;
            self.ruled.clear();
            let waiting = self.describer().is_some();
            self.last = Some(Ruling::Undescribed {
                closed: self.closed(),
                waiting,
            });
            return match waiting {
                true => RepairOutcome::Waiting,
                false => RepairOutcome::Unread,
            };
        }
        self.identify(&sets, access);
        let coming = self.coming();
        let mut outcome = RepairOutcome::Unread;
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
                rebuild::repair_set(
                    &set,
                    &self.bound,
                    coming,
                    weaver_graph::Budget {
                        memory_bytes: Some(budget.memory_or(self.memory_limit)),
                        threads: budget.threads,
                        pool: budget.pool.clone(),
                    },
                    access,
                )
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
        if let Some(short) = self.refused_short() {
            return Some(short);
        }
        match self.last? {
            Ruling::Reached(outcome) => Some(outcome),
            Ruling::Short { closed }
            | Ruling::Undescribed {
                closed,
                waiting: true,
            } => (self.closed() == closed).then_some(RepairOutcome::Waiting),
            Ruling::Undescribed { waiting: false, .. } => Some(RepairOutcome::Unread),
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
