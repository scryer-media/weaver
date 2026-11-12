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
// every arrived run of each volume back when it repairs, a piece at a time,
// leaving its data and recovery blocks where they lie until the rebuild
// spends them. Only packets that lie wholly in a volume's readable ranges
// count: one a hole cuts is dropped, and the scan carries on past it.
//
// A member binds to a described file by its path or bare name, or, when no
// described name answers to it, by its bytes: the repair reads it back and
// identifies it, and the next bind gives it the described name. A described
// file no member is is announced absent, and the repair writes it whole.

pub(crate) mod facts;
mod identity;
mod rebuild;
mod volumes;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use par3_rs::layout::{BlockLayout, FileLayout};
use par3_rs::runtime::ExecutionOptions;
use par3_rs::{
    ChunkDescription, Fingerprint, HEADER_SIZE, MAGIC, Packet, PacketHeader, PacketType, Par3File,
    Par3Set, ScanLimits,
};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome, SetRuling};
use weaver_graph::{
    ByteRange, Claim, Coverage, Delivery, Emit, Extent, Kind, Located, Member, MemberId, Namespace,
    Object, ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use crate::elimination::{self, Length};
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
    // The name it was bound under, which a later volume of the set continues.
    name: Option<String>,
    // How many times what its front reads as has changed.
    changes: u64,
}

impl Volume {
    // The packets the readable spans of the kept front hold, in order.
    fn front_packets(&self) -> Vec<Packet> {
        let mut packets = Vec::new();
        for span in readable(&self.arrived) {
            let end = span.end.min(self.front.len() as u64);
            if let Some(bytes) = self.front.get(span.start as usize..end as usize) {
                scan(bytes, &mut packets);
            }
        }
        packets
    }

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

    // What is known of its length: exact once a byte of it checked.
    fn bound(self) -> Length {
        if self.intact {
            Length::Exact(self.length)
        } else {
            Length::AtMost(self.length)
        }
    }
}

// What a group of unnamed members could be by length: the exact lengths
// of those with a checked byte, and the longest bound of the rest. Any of
// them fits a file exactly when the group does.
#[derive(Debug, Default)]
struct Fitting {
    exact: BTreeSet<u64>,
    at_most: Option<u64>,
}

impl Fitting {
    fn add(&mut self, unnamed: Unnamed) {
        match unnamed.bound() {
            Length::Exact(length) => {
                self.exact.insert(length);
            }
            Length::AtMost(bound) => {
                self.at_most = Some(self.at_most.map_or(bound, |was| was.max(bound)));
            }
        }
    }

    // Whether any member of the group could be a described file of `long`.
    fn fits(&self, long: u64) -> bool {
        self.exact.contains(&long) || self.at_most.is_some_and(|bound| long <= bound)
    }
}

// The lengths of a group of described files. A member fits one of them
// exactly when it fits the group.
#[derive(Debug, Default)]
struct Lengths(BTreeSet<u64>);

impl Lengths {
    fn fitted_by(&self, unnamed: Unnamed) -> bool {
        match unnamed.bound() {
            Length::Exact(length) => self.0.contains(&length),
            Length::AtMost(bound) => self.0.range(..=bound).next().is_some(),
        }
    }
}

// Of `files`, each described path and size in order, those `missing` says
// no member carries or is bound to and no unnamed member may yet turn out
// to be, onto `out` once each. An unnamed member a readback identified may
// be only that file; one not identified may be any it fits. Each file and
// each unnamed member is looked at a bounded number of times, each look
// counted in `work`.
fn absent<'f>(
    files: impl Iterator<Item = (&'f str, u64)>,
    (unnamed, identified): (&BTreeMap<MemberId, Unnamed>, &BTreeMap<MemberId, String>),
    missing: impl Fn(&str) -> bool,
    out: &mut Vec<(String, u64)>,
    work: &mut u64,
) {
    let mut anything = Fitting::default();
    let mut only: BTreeMap<&str, Fitting> = BTreeMap::new();
    for (member, unnamed) in unnamed {
        *work += 1;
        match identified.get(member) {
            None => anything.add(*unnamed),
            Some(path) => only.entry(path).or_default().add(*unnamed),
        }
    }
    let mut listed: BTreeSet<(String, u64)> = out.iter().cloned().collect();
    for (path, size) in files {
        *work += 1;
        let maybe = anything.fits(size) || only.get(path).is_some_and(|group| group.fits(size));
        if missing(path) && !maybe {
            let named = (path.to_owned(), size);
            if !listed.contains(&named) {
                listed.insert(named.clone());
                out.push(named);
            }
        }
    }
}

// A described file as binding by name sees it.
#[derive(Clone, Copy, Debug)]
struct Described<'f> {
    path: &'f str,
    name: &'f str,
    size: u64,
}

impl<'f> Described<'f> {
    fn of(file: &'f par3_rs::Par3File) -> Self {
        Self {
            path: file.path(),
            name: file.name(),
            size: file.size(),
        }
    }
}

// The members' names, each with the first member that carries it and how
// many do. A described file is answered to by the members carrying its
// path or its bare name.
struct Names<'n>(BTreeMap<&'n str, (MemberId, usize)>);

impl<'n> Names<'n> {
    fn of(members: impl Iterator<Item = (&'n str, MemberId)>, work: &mut u64) -> Self {
        let mut names: BTreeMap<&'n str, (MemberId, usize)> = BTreeMap::new();
        for (name, member) in members {
            *work += 1;
            names.entry(name).or_insert((member, 0)).1 += 1;
        }
        Self(names)
    }

    // The names the file answers to that members carry, each once.
    fn answering(&self, file: Described<'_>) -> impl Iterator<Item = (MemberId, usize)> {
        let path = self.0.get(file.path);
        let name = (file.name != file.path)
            .then(|| self.0.get(file.name))
            .flatten();
        path.into_iter().chain(name).copied()
    }

    // The one member that answers to the file, when only one does.
    fn only(&self, file: Described<'_>) -> Option<MemberId> {
        let mut answering = self.answering(file);
        match (answering.next(), answering.next()) {
            (Some((member, 1)), None) => Some(member),
            _ => None,
        }
    }

    // Whether any member answers to the file.
    fn carried(&self, file: Described<'_>) -> bool {
        self.answering(file).next().is_some()
    }
}

// What the members' names say of the described files.
struct ByName<'f> {
    // Each path exactly one member answers to, and that member.
    bound: BTreeMap<String, MemberId>,
    // Every described path, and whether a member answers to it.
    described: BTreeMap<String, bool>,
    // The members no described file answers to whose length fits a file
    // none does.
    unnamed: BTreeMap<MemberId, Unnamed>,
    // The length of each described path, as the first file of it says.
    lengths: BTreeMap<&'f str, u64>,
}

// Binding by name over `files`, every file the sets describe in order, and
// `others`, the namespace's members the object does not own. Each file and
// each member is looked at a bounded number of times, each look counted in
// `work`.
fn by_name<'f, 'm, I>(files: &[Described<'f>], others: impl Fn() -> I, work: &mut u64) -> ByName<'f>
where
    I: Iterator<Item = &'m Member>,
{
    let names = Names::of(
        others().map(|member| (member.name.as_str(), member.id)),
        work,
    );
    let mut bound = BTreeMap::new();
    let mut described = BTreeMap::new();
    // Every path and bare name a described file answers to, and the length
    // of each path.
    let mut answered: BTreeSet<&str> = BTreeSet::new();
    let mut lengths: BTreeMap<&'f str, u64> = BTreeMap::new();
    for &file in files {
        *work += 1;
        if let Some(member) = names.only(file) {
            bound.insert(file.path.to_owned(), member);
        }
        described.insert(file.path.to_owned(), names.carried(file));
        answered.insert(file.path);
        answered.insert(file.name);
        lengths.entry(file.path).or_insert(file.size);
    }
    // The lengths of the files no member answers to.
    let free = Lengths(
        files
            .iter()
            .filter(|file| described.get(file.path) == Some(&false))
            .map(|file| file.size)
            .collect(),
    );
    let unnamed = others()
        .filter(|member| {
            *work += 1;
            !answered.contains(member.name.as_str())
        })
        .filter_map(|member| {
            Some((
                member.id,
                Unnamed::of(member).filter(|u| free.fitted_by(*u))?,
            ))
        })
        .collect();
    ByName {
        bound,
        described,
        unnamed,
        lengths,
    }
}

// The free files the members with no intact byte are by elimination, each
// with the member it is: longest file first, each free file no member is
// takes the member it fits most tightly, once the files those members could
// be are no more than they are.
fn by_elimination(
    free: &[(String, u64)],
    blank: &[(MemberId, Unnamed)],
    work: &mut u64,
) -> Vec<(String, MemberId)> {
    let unnamed = blank
        .iter()
        .map(|(member, unnamed)| (*member, unnamed.bound()))
        .collect();
    elimination::by_elimination(free, unnamed, work)
}

pub struct Par3 {
    id: ObjectId,
    volumes: Vec<Volume>,
    // Described paths and the member each is bound to.
    bound: BTreeMap<String, MemberId>,
    // The members `bound` names, each once.
    covered: BTreeSet<MemberId>,
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
    // Bounded, transient evidence for unchanged accepted replay. A restore
    // with no evidence revalidates its cached ruling on the first feed.
    input: crate::grid::Grid,
    // The repair's decoder allowance when the owner grants it none.
    memory_limit: u64,
    // The namespace's damage as the object was shown it, and the whole
    // blocks of bound files its refusals touch, counted again whenever
    // either changes.
    shown: Shown,
    refused_blocks: u64,
    // What the kept fronts were last read as.
    fronts: Mutex<Fronts>,
}

// The packets each volume's kept front held when it was last scanned, and
// the sets they all described, each with the changes of the fronts it was
// read at.
#[derive(Default)]
struct Fronts {
    packets: Vec<Option<(u64, Vec<Packet>)>>,
    sets: Option<(Vec<u64>, Arc<Vec<Par3Set>>)>,
    // How many fronts were scanned.
    scans: u64,
}

impl Par3 {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            volumes: Vec::new(),
            bound: BTreeMap::new(),
            covered: BTreeSet::new(),
            unnamed: BTreeMap::new(),
            described: BTreeMap::new(),
            identified: BTreeMap::new(),
            needed: 0,
            last: None,
            ruled: Vec::new(),
            settled: BTreeMap::new(),
            input: crate::grid::Grid::default(),
            memory_limit: REPAIR_MEMORY_LIMIT,
            shown: Shown::default(),
            refused_blocks: 0,
            fronts: Mutex::new(Fronts::default()),
        }
    }

    // The described paths and the members they are bound to, with the
    // members they name.
    fn set_bound(&mut self, bound: BTreeMap<String, MemberId>) {
        self.covered = bound.values().copied().collect();
        self.bound = bound;
    }

    fn volume(&mut self, index: VolumeIndex) -> &mut Volume {
        let at = index.0 as usize;
        if self.volumes.len() <= at {
            self.volumes.resize_with(at + 1, Volume::default);
        }
        &mut self.volumes[at]
    }

    // The sets as far as the readable spans of the kept fronts describe them.
    // Only a front that changed since it was last read is scanned again.
    fn front_sets(&self) -> Arc<Vec<Par3Set>> {
        let mut fronts = self.fronts.lock().unwrap_or_else(PoisonError::into_inner);
        let changes: Vec<u64> = self.volumes.iter().map(|volume| volume.changes).collect();
        if let Some((at, sets)) = &fronts.sets
            && *at == changes
        {
            return Arc::clone(sets);
        }
        let Fronts { packets, scans, .. } = &mut *fronts;
        packets.resize_with(self.volumes.len(), || None);
        let mut all = Vec::new();
        for (volume, kept) in self.volumes.iter().zip(packets.iter_mut()) {
            let found = match kept {
                Some((at, found)) if *at == volume.changes => found,
                _ => {
                    *scans += 1;
                    &kept.insert((volume.changes, volume.front_packets())).1
                }
            };
            all.extend(found.iter().cloned());
        }
        let sets = Arc::new(sets(all));
        fronts.sets = Some((changes, Arc::clone(&sets)));
        sets
    }

    fn coming(&self) -> bool {
        self.volumes.iter().any(|volume| !volume.closed)
    }

    // The input blocks whose whole-block extents in the bound files a
    // refused range touches, each once. A file's last, partial block may be
    // packed with another file's, so it is left out: the count never runs
    // ahead of the blocks really lost. Nor is a block whose fingerprint a
    // whole block of a bound file nothing refused carries: equal
    // fingerprints are equal bytes, and the rebuild copies them.
    fn count_refused(&mut self, sets: &[Par3Set]) {
        let mut blocks = 0u64;
        for set in sets {
            let size = set.block_size();
            if size == 0 {
                continue;
            }
            let refused = |member: &MemberId, range: ByteRange| {
                self.shown.refusals.get(member).is_some_and(|ranges| {
                    ranges
                        .iter()
                        .any(|refused| refused.intersect(range).is_some_and(|at| !at.is_empty()))
                })
            };
            let fingerprint = |index: u64| set.block_checksum(index).map(|sum| sum.fingerprint);
            let (lost, kept): (Vec<_>, Vec<_>) = set
                .files()
                .iter()
                .filter_map(|file| Some((self.bound.get(file.path())?, file)))
                .flat_map(|(member, file)| {
                    whole_blocks(file, size).map(move |(range, index)| (member, range, index))
                })
                .partition(|(member, range, _)| refused(member, *range));
            if lost.is_empty() {
                continue;
            }
            let held: BTreeSet<Fingerprint> = kept
                .iter()
                .filter_map(|(_, _, index)| fingerprint(*index))
                .collect();
            let lost: BTreeSet<u64> = lost
                .iter()
                .map(|(_, _, index)| *index)
                .filter(|index| fingerprint(*index).is_none_or(|sum| !held.contains(&sum)))
                .collect();
            blocks += lost.len() as u64;
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
                // Claimed by its name and nothing of it read yet: what it
                // brings is not known.
                _ if !volume.closed && volume.arrived.runs().next().is_none() => None,
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

    // The volume that may describe the sets while damage shows: every
    // volume of a set carries its description ahead of its blocks, so the
    // first still open, an index before any volume and then the one that
    // names the fewest blocks. `None` once none is left to read.
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
        let candidates = identity::Candidates::new(&files, |path| self.free(path));
        let mut read = 0;
        for (&member, unnamed) in &self.unnamed {
            if !unnamed.complete || !unnamed.intact || self.identified.contains_key(&member) {
                continue;
            }
            let len = unnamed.length;
            if let Some(path) = candidates.identify(access, member, len, &mut read) {
                self.identified.insert(member, path.to_owned());
            }
        }
        // A member with no intact byte has nothing to say what it is, so it
        // is what is left, as a PAR2 member no slice named is. The repair
        // makes every byte of it over.
        let taken: BTreeSet<&str> = self.identified.values().map(String::as_str).collect();
        let mut free: Vec<(String, u64)> = files
            .iter()
            .filter(|file| {
                file.len != 0
                    && self.free(&file.path)
                    && !self.bound.contains_key(&file.path)
                    && !taken.contains(file.path.as_str())
            })
            .map(|file| (file.path.clone(), file.len))
            .collect();
        free.sort_by_key(|(_, long)| std::cmp::Reverse(*long));
        free.dedup();
        let blank: Vec<(MemberId, Unnamed)> = self
            .unnamed
            .iter()
            .filter(|(member, unnamed)| {
                unnamed.complete && !unnamed.intact && !self.identified.contains_key(member)
            })
            .map(|(member, unnamed)| (*member, *unnamed))
            .collect();
        let mut work = 0;
        for (path, member) in by_elimination(&free, &blank, &mut work) {
            self.identified.insert(member, path);
        }
        let mut bound = std::mem::take(&mut self.bound);
        bound.extend(self.by_content());
        self.set_bound(bound);
    }

    // A free file of the set that a member still arriving may turn out to
    // be is not missing yet: the set waits for that member, and asks for no
    // recovery on its account.
    fn identifying(&self, set: &Par3Set, arriving: &Fitting) -> bool {
        set.files().iter().any(|file| {
            !self.bound.contains_key(file.path())
                && self.free(file.path())
                && arriving.fits(file.size())
        })
    }

    // The unnamed members still arriving, which `identifying` asks of.
    fn arriving(&self) -> Fitting {
        let mut arriving = Fitting::default();
        for member in self.unnamed.values().filter(|member| !member.complete) {
            arriving.add(*member);
        }
        arriving
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

// The whole blocks a file's chunks map, each as its range in the file and
// its input block index.
fn whole_blocks(file: &Par3File, size: u64) -> impl Iterator<Item = (ByteRange, u64)> + '_ {
    let mut at = 0;
    file.chunks().iter().flat_map(move |chunk| {
        let start = at;
        at += chunk.length();
        let (count, first) = match chunk {
            ChunkDescription::Protected {
                length,
                first_block_index: Some(first),
                ..
            } => (length / size, *first),
            _ => (0, 0),
        };
        (0..count).map(move |step| (ByteRange::at(start + step * size, size), first + step))
    })
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
        volume.changes += 1;
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

    // The kept fronts are not in the facts, nor what waited while the
    // object was out for a repair: each volume's front is asked for again,
    // and all of it past the front that it never heard.
    fn restored(&mut self, out: &mut Vec<Emit>) {
        let past = ByteRange::new(FRONT_CAP as u64, u64::MAX);
        for (index, volume) in self.volumes.iter().enumerate() {
            let mut asked = Coverage::new();
            asked.write(ByteRange::new(0, u64::MAX), weaver_graph::Provenance::Fresh);
            for run in volume.arrived.runs() {
                if let Some(heard) = run.range.intersect(past) {
                    asked.erase(heard);
                }
            }
            let volume = VolumeIndex(index as u32);
            out.extend(asked.runs().map(|run| Emit::Again {
                volume,
                range: run.range,
            }));
        }
    }

    // What arrived there is to come again, and the blocks it carried; a
    // volume closed over them is open until they do.
    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        if let Some(found) = self.volumes.get_mut(volume.0 as usize) {
            found.arrived.erase(range);
            found.closed = false;
            found.changes += 1;
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
    // By the magic its bytes open with, or, before any bytes, by a name
    // that continues the set's: the stem of a volume already held, then a
    // `.vol` name ending in `.par3`.
    fn claim(&mut self, name: &str, first_bytes: &[u8], _length: Option<u64>) -> Claim {
        let ours = if first_bytes.is_empty() {
            self.volumes
                .iter()
                .filter_map(|volume| volume.name.as_deref())
                .any(|held| recovery_volume::same_set(held, name, ".par3"))
        } else {
            first_bytes.get(..MAGIC.len()) == Some(MAGIC.as_slice())
        };
        (ours && self.volumes.len() < MAX_CARRIERS)
            .then_some(VolumeIndex(self.volumes.len() as u32))
            .into_iter()
            .collect()
    }

    // An impossible verdict counted the blocks of the volumes bound then,
    // and an unread one the packets they could bring; a volume bound since
    // may bring more of either, so the object is tried again. Without this
    // the verdict would depend on whether a volume's front decided before
    // or after the damage arrived.
    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        let found = self.volume(volume);
        found.named_blocks = recovery_volume::named_blocks(name, ".par3");
        found.name = Some(name.to_owned());
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
        let files: Vec<Described<'_>> = sets
            .iter()
            .flat_map(Par3Set::files)
            .map(Described::of)
            .collect();
        let id = self.id;
        let ByName {
            mut bound,
            described,
            unnamed,
            lengths,
        } = by_name(
            &files,
            || {
                namespace
                    .members()
                    .filter(move |member| member.bound_to != Some(id))
            },
            &mut 0,
        );
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
            let Some(&length) = lengths.get(path.as_str()) else {
                continue;
            };
            if namespace.recognise(member, &path, length).is_some() {
                renamed.push(member);
            }
            bound.insert(path, member);
        }
        // A member bound under its own name keeps it, and takes the
        // described length as `Namespace::take_described` says, once the
        // set's blocks verify.
        let verified = matches!(
            self.last,
            Some(Ruling::Reached(
                RepairOutcome::Clean | RepairOutcome::Repaired { .. }
            ))
        );
        for (path, member) in &bound {
            let Some(&size) = lengths.get(path.as_str()) else {
                continue;
            };
            if namespace.take_described(*member, size, verified) {
                renamed.push(*member);
            }
        }
        if learned || bound != self.bound {
            self.last = None;
        }
        self.set_bound(bound);
        self.count_refused(&sets);
    }

    // Every described file no member carries by name and none may yet turn
    // out to be by content, by path and size, once each. A member of the
    // file's size that no described name answers to may be it until a
    // readback says what it is.
    fn absent(&self, out: &mut Vec<(String, u64)>) {
        let sets = self.front_sets();
        let files = sets
            .iter()
            .flat_map(Par3Set::files)
            .map(|file| (file.path(), file.size()));
        absent(
            files,
            (&self.unnamed, &self.identified),
            |path| self.free(path) && !self.bound.contains_key(path),
            out,
            &mut 0,
        );
    }

    // No front read so far describes a file: the description is in a volume
    // the root has only sampled the head of.
    fn undescribed(&self) -> bool {
        self.described.is_empty()
    }

    fn covers(&self, member: MemberId) -> bool {
        self.covered.contains(&member)
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        if self.covers(member) && !self.input.matches(member, segments) {
            self.last = None;
            self.settled
                .retain(|_, settled| !settled.members.contains(&Some(member)));
            self.ruled.clear();
        }
        self.feed_proven(member, segments);
    }

    fn feed_proven(&mut self, member: MemberId, segments: &[Segment]) {
        self.input.offer_all(member, crate::grid::WHOLE, segments);
    }

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
    // volume that may describe them is, all of it, then the next once that
    // one is read through without one. Otherwise every volume whose head
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
        // Every readable span of every volume, scanned one at a time a piece
        // at a time; a span that does not read back whole is left out. The
        // data and recovery blocks stay in the volumes until spent.
        let spans = self
            .volumes
            .iter()
            .enumerate()
            .flat_map(|(index, volume)| {
                readable(&volume.arrived)
                    .into_iter()
                    .map(move |span| (VolumeIndex(index as u32), span))
            })
            .collect();
        let volumes::Read {
            sets,
            payloads,
            volumes,
        } = volumes::read(spans, ScanLimits::default(), MAX_SETS, access);
        // With no set described by anything read, the object waits on a
        // volume that may carry a description while damage shows; with no
        // damage, or none left open, it checked nothing.
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
        let arriving = self.arriving();
        for (set, payloads) in sets.into_iter().zip(payloads) {
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
            let result = if self.identifying(&set, &arriving) {
                rebuild::SetRepair {
                    outcome: RepairOutcome::Waiting,
                    lost: 0,
                }
            } else {
                rebuild::repair_set(
                    (&set, &payloads, &volumes),
                    &self.bound,
                    coming,
                    weaver_graph::Budget {
                        memory_bytes: Some(budget.memory_or(self.memory_limit)),
                        window_bytes: budget.window_bytes,
                        window_reserved: budget.window_reserved,
                        threads: budget.threads,
                        pool: budget.pool.clone(),
                        lender: budget.lender.clone(),
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
                blocks_reachable: rebuild::usable_recovery(&set, &payloads).len() as u64,
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
