// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR recovery volume set: the standalone `.rev` files RAR writes beside
// a volume set, RAR5 or RAR3. It lives in the namespace of the set's
// volumes, as a PAR2 set posted beside them does, claims every `.rev` of the
// set as a volume of its own, and covers the data volumes the set's naming
// puts in it, one slice per volume. It is never delivered.
//
// It rules once the volumes are in: clean when every one is whole; a
// restore when no more are missing or damaged than it holds recovery
// volumes, which unrar-rs runs over files staged for it and which rebuilds
// each bad volume whole; impossible otherwise.

mod header;
mod restore;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Extent, Facts, Kind, Located, MemberId, Namespace, Object,
    ObjectId, Placement, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex, Weight,
};

use self::header::{Header, RAR3_FOOTER, Read};
use self::restore::{Source, Staged};
use crate::engine::SetName;
use crate::rar_engine::naming;

const REV5_SIGN: &[u8] = b"Rar!\x1aRev";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Rev {
    name: String,
    // The volume's contiguous front, as far as a RAR5 header needs it.
    front: Vec<u8>,
    // How much of the front the header needs; zero once it is read or the
    // volume is no RAR5 one.
    wanted: usize,
    // The RAR3 footer, byte by byte as it arrives.
    footer: [Option<u8>; RAR3_FOOTER],
    length: Option<u64>,
    closed: bool,
    lost: bool,
}

impl Rev {
    fn new() -> Self {
        Self {
            wanted: REV5_SIGN.len(),
            ..Self::default()
        }
    }

    // Whole in hand: nothing more comes and nothing of it was lost.
    fn usable(&self) -> bool {
        self.closed && !self.lost && self.length.is_some()
    }

    // The header tells how much of the front it needs only as the front
    // grows, so the front takes from these bytes for as long as each read of
    // it asks for more and they reach: what it takes never depends on where
    // the deliveries were cut.
    fn take(&mut self, offset: u64, bytes: &[u8]) {
        let end = offset + bytes.len() as u64;
        while self.wanted > self.front.len() {
            let have = self.front.len() as u64;
            if offset > have || end <= have {
                break;
            }
            let fresh = &bytes[(have - offset) as usize..];
            let room = self.wanted - self.front.len();
            self.front
                .extend_from_slice(&fresh[..fresh.len().min(room)]);
            self.wanted = match header::rar5(&self.front) {
                Read::Short(needed) => needed,
                Read::Header(_) | Read::Not => 0,
            };
        }
        if let Some(length) = self.length {
            let start = length.saturating_sub(RAR3_FOOTER as u64);
            for at in offset.max(start)..end.min(length) {
                self.footer[(at - start) as usize] = Some(bytes[(at - offset) as usize]);
            }
        }
    }

    // Where a RAR3 volume's footer starts while it is still to come: a
    // volume that is no RAR5 one says what it is at its end, so that much
    // of it is wanted however the rest waits.
    fn footer_wanted(&self) -> Option<u64> {
        let length = self.length?;
        let rar3 =
            matches!(header::rar5(&self.front), Read::Not) && !self.front.starts_with(REV5_SIGN);
        (rar3 && self.footer.iter().any(Option::is_none))
            .then(|| length.saturating_sub(RAR3_FOOTER as u64))
    }

    fn header(&self) -> Option<Header> {
        match header::rar5(&self.front) {
            Read::Header(header) => return Some(header),
            Read::Short(_) => return None,
            Read::Not => {}
        }
        if self.front.starts_with(REV5_SIGN) {
            return None;
        }
        let mut footer = [0; RAR3_FOOTER];
        for (byte, have) in footer.iter_mut().zip(self.footer) {
            *byte = have?;
        }
        header::rar3(&footer, self.length?)
    }
}

#[derive(Serialize, Deserialize)]
pub struct RarRecovery {
    id: ObjectId,
    revs: Vec<Rev>,
    // The set the recovery volumes' names cover, as the set's naming gives
    // it.
    stem: Option<String>,
    // The set's data volumes in the namespace, by their index in the set.
    bound: BTreeMap<u32, MemberId>,
    // Their names, which the restore stages them under.
    names: BTreeMap<MemberId, String>,
    // A data volume's name, to name the ones no member carries after.
    reference: Option<String>,
    // The bound volumes that were whole at the last bind.
    whole: BTreeSet<u32>,
    // What the last repair came to, for the members bound then.
    last: Option<Ruling>,
    // Members of the namespace a range of which the parent refused: a data
    // volume among them is missing for good.
    refused: BTreeSet<MemberId>,
}

// The verdict a repair reached, or that it came up short of recovery
// volumes: it needs as many in hand as it found data volumes missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Ruling {
    Reached(RepairOutcome),
    Short { needed: u64 },
}

impl Durable for RarRecovery {
    // 2: a repair that came up short kept with the volumes it needs.
    // 3: the members the parent refused a range of.
    const VERSION: u16 = 3;
}

impl RarRecovery {
    pub fn new(id: ObjectId) -> Self {
        Self {
            id,
            revs: Vec::new(),
            stem: None,
            bound: BTreeMap::new(),
            names: BTreeMap::new(),
            reference: None,
            whole: BTreeSet::new(),
            last: None,
            refused: BTreeSet::new(),
        }
    }

    fn rev(&mut self, volume: VolumeIndex) -> &mut Rev {
        let at = volume.0 as usize;
        if self.revs.len() <= at {
            self.revs.resize_with(at + 1, Rev::new);
        }
        &mut self.revs[at]
    }

    // What the set's recovery volumes say of it: the first header with a
    // table of sizes, else any.
    fn header(&self) -> Option<Header> {
        let mut found: Option<Header> = None;
        for header in self.revs.iter().filter_map(Rev::header) {
            if !header.sizes.is_empty() {
                return Some(header);
            }
            found.get_or_insert(header);
        }
        found
    }

    // Each data volume's size as the set's header describes it, by index: a
    // RAR5 table gives every one, and a RAR3 footer every one but the last,
    // which falls short of the width the others share.
    fn described_sizes(&self) -> Vec<(u32, u64)> {
        let Some(header) = self.header() else {
            return Vec::new();
        };
        let described = header
            .sizes
            .len()
            .saturating_sub(usize::from(!header.last_described));
        header
            .sizes
            .into_iter()
            .take(described)
            .enumerate()
            .map(|(index, size)| (index as u32, size))
            .collect()
    }

    // Every data volume of the set, by index: as many as the header counts,
    // or as the names reach.
    fn slots(&self) -> u32 {
        let named = self.bound.keys().next_back().map_or(0, |last| last + 1);
        self.header()
            .map_or(named, |header| header.data_count.max(named))
    }

    fn missing(&self) -> u64 {
        (0..self.slots())
            .filter(|index| !self.whole.contains(index))
            .count() as u64
    }

    fn usable(&self) -> u64 {
        self.revs.iter().filter(|rev| rev.usable()).count() as u64
    }

    fn coming(&self) -> bool {
        self.revs.iter().any(|rev| !rev.closed)
    }

    // The set's ruling on what was refused: a data volume a range of which
    // will never come needs a recovery volume of its own, so the set is
    // impossible once those outnumber the recovery volumes in hand and
    // still to come whole.
    fn refused_short(&self) -> Option<RepairOutcome> {
        let damaged = self
            .bound
            .values()
            .filter(|member| self.refused.contains(member))
            .count() as u64;
        let reachable = self
            .revs
            .iter()
            .filter(|rev| rev.usable() || (!rev.closed && !rev.lost))
            .count() as u64;
        (damaged > reachable).then_some(RepairOutcome::Impossible { damaged, reachable })
    }

    // The set staged for unrar-rs: every data volume whose member has a
    // length, and every recovery volume in hand.
    fn staged(&self, access: &dyn MemberAccess) -> Vec<Staged> {
        let data = self.bound.values().filter_map(|&member| {
            Some(Staged {
                name: String::new(),
                source: Source::Member(member),
                length: access.length(member)?,
            })
        });
        let revs = self.revs.iter().enumerate().filter_map(|(index, rev)| {
            Some(Staged {
                name: rev.name.clone(),
                source: Source::Volume(VolumeIndex(index as u32)),
                length: rev.length.filter(|_| rev.usable())?,
            })
        });
        data.chain(revs).collect()
    }

    fn rule(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let missing = self.missing();
        if missing == 0 {
            return RepairOutcome::Clean;
        }
        let usable = self.usable();
        if missing > usable {
            return if self.coming() {
                RepairOutcome::Waiting
            } else {
                RepairOutcome::Impossible {
                    damaged: missing,
                    reachable: usable,
                }
            };
        }
        let impossible = RepairOutcome::Impossible {
            damaged: missing,
            reachable: usable,
        };
        let Some(dir) = access.scratch() else {
            return impossible;
        };
        let mut staged = self.staged(access);
        for file in &mut staged {
            if let Source::Member(member) = file.source {
                file.name = self.name_of(member);
            }
        }
        let Some(rebuilt) = restore::restore(&dir, &staged, access) else {
            return impossible;
        };
        let mut written = 0;
        for (index, path) in rebuilt {
            if let Some(&member) = self.bound.get(&index)
                && restore::write_back(&path, member, access)
            {
                written += 1;
            }
        }
        if written == 0 {
            RepairOutcome::Clean
        } else {
            RepairOutcome::Repaired { slices: written }
        }
    }

    fn name_of(&self, member: MemberId) -> String {
        self.names.get(&member).cloned().unwrap_or_default()
    }
}

impl Object for RarRecovery {
    fn kind(&self) -> Kind {
        Kind::RarRecovery
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
        self.rev(delivery.volume)
            .take(delivery.offset, delivery.bytes);
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, _out: &mut Vec<Emit>) {
        self.rev(volume).length = Some(length);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.rev(volume).closed = true;
    }

    fn volume_lost(&mut self, volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {
        self.rev(volume).lost = true;
    }

    fn wants(&self, out: &mut Vec<Seek>) {
        Recovery::wants(self, out);
    }

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The volume stays in its member's own file, where the restore reads it.
    // The front and footer it copies out are in its facts, which a restart
    // keeps: it says nothing of the posted bytes.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

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

    // Every recovery volume of the set this one covers, numbered as it
    // comes. The first is the one the object was built for. Bytes outrank
    // names: a front that signs another kind is that kind's, whatever its
    // name; one that signs nothing, as a RAR3 volume's does, is claimed by
    // its name.
    fn claim(
        &mut self,
        name: &str,
        first_bytes: &[u8],
        length: Option<u64>,
    ) -> Option<VolumeIndex> {
        if matches!(
            weaver_graph::signed(first_bytes, length),
            weaver_graph::Sniff::Object(kind) if kind != Kind::RarRecovery
        ) {
            return None;
        }
        let rev = first_bytes.starts_with(REV5_SIGN) || naming::recovery_stem(name).is_some();
        (rev && !self.revs.is_empty()
            && self.stem.is_some()
            && naming::recovery_stem(name) == self.stem)
            .then_some(VolumeIndex(self.revs.len() as u32))
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], _length: Option<u64>) {
        if self.stem.is_none() {
            self.stem = naming::recovery_stem(name);
        }
        let rev = self.rev(volume);
        rev.name = name.to_owned();
        rev.take(0, front);
    }

    fn renamed(&mut self, volume: VolumeIndex, name: &str) {
        self.rev(volume).name = name.to_owned();
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }

    fn facts(&self) -> Facts {
        Facts::of(Kind::RarRecovery, self)
    }
}

impl Recovery for RarRecovery {
    // The data volumes the set's naming puts in the set, one member per
    // index; the first a name gives an index keeps it. The set is the one
    // with the longest name the first recovery volume's name covers: a set
    // called `set2` owns `set21.rev` ahead of one called `set`.
    fn bind(&mut self, namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {
        let Some(rev) = self.revs.first().filter(|_| self.stem.is_some()) else {
            return;
        };
        let stem = namespace
            .members()
            .filter(|member| member.bound_to != Some(self.id))
            .filter_map(|member| naming::parse(&member.name))
            .map(|volume| volume.stem().to_owned())
            .filter(|stem| naming::recovery_covers(&rev.name, stem))
            .max_by_key(String::len);
        let mut bound = BTreeMap::new();
        let mut names = BTreeMap::new();
        for member in namespace
            .members()
            .filter(|member| member.bound_to != Some(self.id))
        {
            let Some(volume) =
                naming::parse(&member.name).filter(|name| Some(name.stem()) == stem.as_deref())
            else {
                continue;
            };
            if let std::collections::btree_map::Entry::Vacant(slot) = bound.entry(volume.index()) {
                slot.insert(member.id);
                names.insert(member.id, member.name.clone());
            }
        }
        // A volume the set repaired that is longer than its table gives, with
        // no good bytes past that size, takes the table's size: what lies
        // past was booked for articles that never came, and the rebuilt
        // volume is the size the table gives.
        let repaired = matches!(
            self.last,
            Some(Ruling::Reached(RepairOutcome::Repaired { .. }))
        );
        let sizes = if repaired {
            self.described_sizes()
        } else {
            Vec::new()
        };
        for (index, size) in sizes {
            if let Some(member) = bound.get(&index) {
                namespace.trim_booked(*member, size);
            }
        }
        let whole: BTreeSet<u32> = bound
            .iter()
            .filter(|(_, member)| {
                namespace
                    .get(**member)
                    .is_some_and(|record| record.is_whole())
            })
            .map(|(index, _)| *index)
            .collect();
        // A ruling is on the members bound, not on how whole they were: a
        // volume its own repair made whole leaves it standing.
        if bound != self.bound {
            self.last = None;
        }
        self.reference = names.values().next().cloned();
        self.bound = bound;
        self.names = names;
        self.whole = whole;
    }

    // The data volumes the header counts that no member carries, named in
    // the set's own numbering, at the size the header gives.
    fn absent(&self, out: &mut Vec<(String, u64)>) {
        let (Some(header), Some(reference)) = (self.header(), self.reference.as_deref()) else {
            return;
        };
        for (index, size) in header.sizes.iter().enumerate() {
            let index = index as u32;
            if self.bound.contains_key(&index) {
                continue;
            }
            if let Some(name) = naming::sibling(reference, index) {
                out.push((name, *size));
            }
        }
    }

    fn covers(&self, member: MemberId) -> bool {
        self.bound.values().any(|bound| *bound == member)
    }

    // A volume is checked whole, by the restore; nothing is folded as
    // bytes pass.
    fn feed(&mut self, _member: MemberId, _segments: &[Segment]) {}

    // One slice per volume.
    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        let Some((index, _)) = self.bound.iter().find(|(_, bound)| **bound == member) else {
            return;
        };
        out.push(match self.outcome() {
            _ if self.whole.contains(index) => SliceVerdict::Intact,
            Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. }) => SliceVerdict::Intact,
            Some(RepairOutcome::Impossible { .. }) => SliceVerdict::Damaged,
            _ => SliceVerdict::Unknown,
        });
    }

    // A recovery volume is spent whole. Once its front is in, the rest
    // waits deferred (what is in hand of it the root leaves be) until the
    // set comes up short, and then as many volumes as the set is short of
    // are wanted whole. Nothing of the deferral is kept: the same ruling
    // says it again on every round, after a restart too.
    fn wants(&self, out: &mut Vec<Seek>) {
        let mut short = match self.last {
            Some(Ruling::Short { needed }) => needed.saturating_sub(self.usable()),
            _ => 0,
        };
        for (index, rev) in self.revs.iter().enumerate() {
            let volume = VolumeIndex(index as u32);
            if rev.closed {
                continue;
            }
            if short > 0 && !rev.lost {
                short -= 1;
                out.push(Seek::recovery(
                    volume,
                    ByteRange::new(0, u64::MAX),
                    Weight::FIRST,
                ));
                continue;
            }
            if rev.front.is_empty() {
                continue;
            }
            let end = match rev.footer_wanted() {
                Some(start) => {
                    out.push(Seek::recovery(
                        volume,
                        ByteRange::new(start, start + RAR3_FOOTER as u64),
                        Weight::FIRST,
                    ));
                    start
                }
                None => u64::MAX,
            };
            out.push(Seek::deferred(volume, ByteRange::new(0, end)));
        }
    }

    // Kept for every member of the namespace, bound or not: a bind later
    // counts what was refused before.
    fn refused(&mut self, member: MemberId, _range: ByteRange) {
        self.refused.insert(member);
    }

    fn repair(&mut self, access: &mut dyn MemberAccess, _: weaver_graph::Budget) -> RepairOutcome {
        if let Some(short) = self.refused_short() {
            return short;
        }
        let outcome = self.rule(access);
        self.last = Some(match outcome {
            RepairOutcome::Waiting => Ruling::Short {
                needed: self.missing(),
            },
            reached => Ruling::Reached(reached),
        });
        outcome
    }

    // A recovery volume is spent whole, so one is in hand once it closes
    // unlost; a wait is over once as many are as it needs, or none more
    // can come.
    fn outcome(&self) -> Option<RepairOutcome> {
        if let Some(short) = self.refused_short() {
            return Some(short);
        }
        match self.last? {
            Ruling::Reached(outcome) => Some(outcome),
            Ruling::Short { needed } => {
                (self.usable() < needed && self.coming()).then_some(RepairOutcome::Waiting)
            }
        }
    }
}
