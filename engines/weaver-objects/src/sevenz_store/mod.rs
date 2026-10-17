// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z store: a 7z container, whole or split into `-v` volumes, streamed
// into its members as its bytes arrive, in any order, with no engine and no
// file of its own. It reads what it needs and nothing else: the signature
// header, then the end header, which it bids for out of order because it
// sits at the container's tail, then the map the header gives. Stored
// folders map straight to members; AES folders map through the cipher once
// a password is proved. Anything that compresses goes to an engine.
//
// The object is pure: no I/O, no threads, no files. Bytes it cannot place yet
// are held by its owner, who hands them back once the map places them.

mod geometry;
mod header;
mod kept;
mod map;
mod open;
mod place;
mod record;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use sevenz_turbo::Password;
use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Kind, MemberId, Object, Placement, Pull,
    RecoveryKind, Seek, VolumeIndex, Weight, Why,
};
use zeroize::Zeroizing;

use crate::crypt::Cbc;
use crate::engine::{Format, SetName};
use crate::grid::{Grid, crc32};
use crate::keys::Keys;
use crate::sevenz_engine::SevenZip;
use geometry::Geometry;
use header::{SIGNATURE, Signature, Start};
use kept::Kept;
use map::Map;

// The one envelope: container coordinates, for every byte that belongs to no
// member. Bytes a volume carries past what the geometry gives it go to the
// envelope after it, one per volume, which holds nothing.
const ENVELOPE: u32 = 0;

// What may follow the container in its last volume.
const RECOVERY: [(&[u8; 8], RecoveryKind); 2] = [
    (b"PAR2\0PKT", RecoveryKind::Par2),
    (b"PAR3\0PKT", RecoveryKind::Par3),
];

// A folder's cipher, by what the object knows of its key.
enum Lock {
    Plain,
    Locked,
    Open(Box<Cbc>),
}

impl Lock {
    fn is_locked(&self) -> bool {
        matches!(self, Lock::Locked)
    }
}

// What has been said for one member.
#[derive(Default)]
struct Out {
    coverage: Coverage,
    closed: bool,
    damaged: bool,
}

// A password offered, and what it was found to open.
struct Candidate {
    secret: Zeroizing<String>,
    // Kept per candidate so every derivation sevenz-turbo runs for it shares
    // the password's own cache.
    password: Password,
    header_wrong: bool,
    folder_wrong: bool,
}

pub struct SevenZipStore {
    keys: Arc<Keys>,
    names: BTreeMap<VolumeIndex, String>,
    lengths: BTreeMap<VolumeIndex, u64>,
    kept: Kept,
    start: Option<Start>,
    geometry: Option<Geometry>,
    // The end header is known to be encrypted.
    encrypted: bool,
    // Container ranges the header parse needs, the end header among them.
    header_ranges: Vec<ByteRange>,
    map: Option<Map>,
    // The header bytes the map was built on, by range and checksum.
    basis: Vec<(ByteRange, u32)>,
    folders: Vec<Lock>,
    members: Vec<Out>,
    grid: Grid,
    candidates: Vec<Candidate>,
    // The candidate that opened the header or the folders.
    opened: Option<usize>,
    proof: Option<open::Proof>,
    asked: bool,
    // Container ranges wanted ahead of the normal order, and the volume
    // ranges already bid for them.
    bids: Vec<(ByteRange, Weight)>,
    bid: BTreeSet<(u32, u64, u64)>,
    damage_said: HashSet<(u64, u64, u32)>,
    lengths_said: BTreeSet<VolumeIndex>,
    recovery_said: bool,
    // Something changed since the object last looked.
    stale: bool,
    // The object gave its container up; it says nothing more.
    gone: bool,
}

impl Default for SevenZipStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SevenZipStore {
    pub fn new() -> Self {
        Self::with_keys(Keys::global())
    }

    // With a key cache of its own, so a test counts its own derivations.
    pub fn with_keys(keys: Arc<Keys>) -> Self {
        let mut kept = Kept::default();
        kept.want(SIGNATURE);
        Self {
            keys,
            names: BTreeMap::new(),
            lengths: BTreeMap::new(),
            kept,
            start: None,
            geometry: None,
            encrypted: false,
            header_ranges: Vec::new(),
            map: None,
            basis: Vec::new(),
            folders: Vec::new(),
            members: Vec::new(),
            grid: Grid::default(),
            candidates: Vec::new(),
            opened: None,
            proof: None,
            asked: false,
            bids: Vec::new(),
            bid: BTreeSet::new(),
            damage_said: HashSet::new(),
            lengths_said: BTreeSet::new(),
            recovery_said: false,
            stale: true,
            gone: false,
        }
    }

    // A volume range in container coordinates. Volume 0 starts the
    // container whatever the geometry; the others need it.
    fn container(&self, volume: VolumeIndex, range: ByteRange) -> Option<ByteRange> {
        match self.geometry {
            Some(geometry) => geometry.to_container(volume, range),
            None => (volume.0 == 0).then_some(range),
        }
    }

    fn base(&self, volume: VolumeIndex) -> u64 {
        self.geometry.map_or(0, |geometry| geometry.base(volume))
    }

    fn to_volumes(&self, range: ByteRange, out: &mut Vec<(VolumeIndex, ByteRange)>) {
        match self.geometry {
            Some(geometry) => geometry.to_volumes(range, out),
            None => out.push((VolumeIndex(0), range)),
        }
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    fn bid(&mut self, range: ByteRange, weight: Weight) {
        if !range.is_empty() && !self.bids.contains(&(range, weight)) {
            self.bids.push((range, weight));
        }
    }

    // A header range failed its checksum: say so once for each version of
    // its bytes, then wait for a repair to deliver it again.
    fn say_damaged(&mut self, range: ByteRange, crc: u32, out: &mut Vec<Emit>) {
        if !self.damage_said.insert((range.start, range.end, crc)) {
            return;
        }
        let mut pieces = Vec::new();
        self.to_volumes(range, &mut pieces);
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Emit::Damaged { volume, range }),
        );
    }

    // Look again at everything, after anything changed.
    fn settle(&mut self, out: &mut Vec<Emit>) {
        if self.gone || !std::mem::take(&mut self.stale) {
            return;
        }
        if self.start.is_none() && !self.read_signature(out) {
            return;
        }
        let Some(start) = self.start else {
            return;
        };
        if self.geometry.is_none()
            && let Some(first) = self.lengths.get(&VolumeIndex(0)).copied()
        {
            match Geometry::new(first, start.total()) {
                Some(geometry) => self.geometry = Some(geometry),
                None => return self.give_up(Emit::NeedsEngine(Why::Unsupported), out),
            }
        }
        self.check_lengths(out);
        self.check_recovery(start, out);
        if self.map.is_none() {
            self.read_header(start, out);
        }
        if self.map.is_some() && !self.gone {
            self.open_folders(out);
        }
    }

    fn read_signature(&mut self, out: &mut Vec<Emit>) -> bool {
        let Some(bytes) = self.kept.read(SIGNATURE) else {
            return false;
        };
        match header::signature(bytes) {
            Signature::Foreign => {
                self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
                false
            }
            Signature::Damaged => {
                let crc = crc32(bytes);
                self.say_damaged(SIGNATURE, crc, out);
                false
            }
            Signature::Start(start) => {
                self.start = Some(start);
                self.kept.want(start.end_header);
                self.header_ranges.push(start.end_header);
                true
            }
        }
    }

    // Every volume but the last as long as the first; the last at least as
    // long as the rest of the container.
    fn check_lengths(&mut self, out: &mut Vec<Emit>) {
        let Some(geometry) = self.geometry else {
            return;
        };
        for (volume, length) in &self.lengths {
            if geometry.agrees(*volume, *length) || !self.lengths_said.insert(*volume) {
                continue;
            }
            let expected = if *volume == geometry.last() {
                geometry.total().saturating_sub(geometry.base(*volume))
            } else {
                geometry.base(VolumeIndex(1))
            };
            out.push(Emit::Damaged {
                volume: *volume,
                range: ByteRange::new(0, (*length).max(expected)),
            });
        }
    }

    // Recovery data appended after the container, in its last volume.
    fn check_recovery(&mut self, start: Start, out: &mut Vec<Emit>) {
        let Some(geometry) = self.geometry else {
            return;
        };
        let last = geometry.last();
        let Some(length) = self.lengths.get(&last).copied() else {
            return;
        };
        if self.recovery_said {
            return;
        }
        let after = start.total() - geometry.base(last);
        if length <= after {
            self.recovery_said = true;
            return;
        }
        let probe = ByteRange::at(start.total(), (length - after).min(8));
        self.kept.want(probe);
        self.bid(probe, Weight::FIRST);
        let Some(bytes) = self.kept.read(probe) else {
            return;
        };
        self.recovery_said = true;
        if let Some((_, kind)) = RECOVERY.iter().find(|(magic, _)| bytes == magic.as_slice()) {
            out.push(Emit::RecoveryAt {
                volume: last,
                range: ByteRange::new(after, length),
                kind: *kind,
            });
        }
    }

    fn read_header(&mut self, start: Start, out: &mut Vec<Emit>) {
        let end = start.end_header;
        let Some(bytes) = self.kept.read(end) else {
            let mut missing = Vec::new();
            self.kept.missing(end, &mut missing);
            for range in missing {
                self.bid(range, Weight::FIRST);
            }
            return;
        };
        let crc = crc32(bytes);
        if crc != start.crc {
            return self.say_damaged(end, crc, out);
        }
        let total = start.total();
        if !self.encrypted {
            match header::parse(&self.kept, total, None, false) {
                header::Parsed::Missing(range) => return self.want_header(range),
                header::Parsed::Unverified => return,
                header::Parsed::Broken => {
                    return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
                }
                header::Parsed::Locked => self.encrypted = true,
                header::Parsed::Archive { archive, read } => {
                    return self.build(&archive, &read, out);
                }
            }
        }
        self.open_header(start, out);
    }

    fn want_header(&mut self, range: ByteRange) {
        self.kept.want(range);
        self.header_ranges.push(range);
        self.bid(range, Weight::FIRST);
        // The bytes may be held already, or arrive with the next delivery.
        self.stale = true;
    }

    fn build(&mut self, archive: &sevenz_turbo::Archive, read: &[ByteRange], out: &mut Vec<Emit>) {
        let map = match map::build(archive) {
            Ok(map) => map,
            Err(map::Shape::Compressed) => {
                return self.give_up(Emit::NeedsEngine(Why::Compressed), out);
            }
            Err(map::Shape::Unsupported) => {
                return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            }
        };
        self.basis = read
            .iter()
            .filter_map(|range| self.kept.read(*range).map(|bytes| (*range, crc32(bytes))))
            .collect();
        self.folders = map
            .folders
            .iter()
            .map(|folder| match folder.aes {
                Some(_) => Lock::Locked,
                None => Lock::Plain,
            })
            .collect();
        self.members = map.members.iter().map(|_| Out::default()).collect();
        // Every member is announced now, in id order, which is the order the
        // namespace numbers them in.
        for (id, index) in map.members.iter().enumerate() {
            let entry = &map.entries[*index];
            let member = MemberId(id as u32);
            out.push(Emit::Member {
                member,
                name: entry.name.clone(),
                length: Some(entry.length),
            });
            if entry.length == 0 {
                self.members[id].closed = true;
                out.push(Emit::MemberClosed(member));
            }
        }
        if self.opened.is_none() && self.folders.iter().any(Lock::is_locked) {
            self.proof = open::Proof::choose(&map);
            if let Some(proof) = &self.proof {
                let range = proof.range;
                self.kept.want(range);
                self.bid(range, Weight(1));
            }
        }
        self.map = Some(map);
    }

    // Repaired bytes changed the header the map was built on: everything
    // said about the members was built on wrong bytes.
    fn rebuild_if_changed(&mut self, range: ByteRange, out: &mut Vec<Emit>) {
        if self.map.is_none()
            || !self
                .basis
                .iter()
                .any(|(basis, _)| basis.intersect(range).is_some())
        {
            return;
        }
        let same = self.basis.iter().all(|(basis, crc)| {
            self.kept
                .read(*basis)
                .is_some_and(|bytes| crc32(bytes) == *crc)
        });
        if same {
            return;
        }
        for id in 0..self.members.len() {
            out.push(Emit::Retract(MemberId(id as u32)));
        }
        self.map = None;
        self.basis.clear();
        self.folders.clear();
        self.members.clear();
        self.grid = Grid::default();
        self.proof = None;
    }
}

impl Object for SevenZipStore {
    fn kind(&self) -> Kind {
        Kind::SevenZipStore
    }

    fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        let Some(range) = self.container(delivery.volume, delivery.range()) else {
            return;
        };
        if range.is_empty() {
            return;
        }
        let skip = (range.start - self.base(delivery.volume)) - delivery.offset;
        let bytes = &delivery.bytes[skip as usize..(skip + range.len()) as usize];
        // Absorbing can make the object want more of this very delivery: the
        // signature names the end header, the end header names its packs.
        loop {
            let generation = self.kept.generation();
            if self.kept.absorb(range, bytes, delivery.provenance) {
                self.stale = true;
                self.rebuild_if_changed(range, out);
            }
            self.settle(out);
            if self.gone {
                return;
            }
            if self.kept.generation() == generation {
                break;
            }
        }
        self.route(range, bytes, delivery.provenance, out);
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        self.stale = true;
        self.settle(out);
        let Some(geometry) = self.geometry else {
            return;
        };
        if self.gone || volume != geometry.last() {
            return;
        }
        // Everything that could arrive has: a password that has not opened
        // the container by now never will.
        let header_shut = self.map.is_none()
            && self.encrypted
            && self
                .candidates
                .iter()
                .all(|candidate| candidate.header_wrong);
        let folders_shut = self.map.is_some()
            && self.asked
            && self.folders.iter().any(Lock::is_locked)
            && self
                .candidates
                .iter()
                .all(|candidate| candidate.folder_wrong);
        if header_shut || folders_shut {
            self.give_up(Emit::Refuse(Why::EncryptedHeader), out);
        }
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        let Some(range) = self.container(volume, range) else {
            return;
        };
        let headers = self.map.is_none()
            && (SIGNATURE.intersect(range).is_some()
                || self
                    .header_ranges
                    .iter()
                    .any(|header| header.intersect(range).is_some()));
        if headers {
            return self.give_up(Emit::Refuse(Why::Unrepairable), out);
        }
        // A proof that cannot arrive proves nothing: admit unproven.
        if self
            .proof
            .as_ref()
            .is_some_and(|proof| proof.range.intersect(range).is_some())
        {
            self.proof = None;
            self.stale = true;
            self.settle(out);
        }
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        if self.gone {
            return;
        }
        let mut pieces = Vec::new();
        for (range, weight) in self.bids.clone() {
            // Only volumes bound so far can be bid: a seek into one not yet
            // claimed would be answered as unseekable.
            if self.geometry.is_none() {
                break;
            }
            pieces.clear();
            self.to_volumes(range, &mut pieces);
            for (volume, range) in &pieces {
                if self.names.contains_key(volume)
                    && self.bid.insert((volume.0, range.start, range.end))
                {
                    out.push(Seek::data(*volume, *range, weight));
                }
            }
        }
    }

    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if self.gone || pull != Pull::Unseekable || self.map.is_some() {
            return;
        }
        let Some(range) = self.container(seek.volume, seek.range) else {
            return;
        };
        // The header is at the tail, and nothing above can bring the tail
        // forward: everything ahead of it would have to be held.
        if self
            .header_ranges
            .iter()
            .any(|header| header.intersect(range).is_some())
        {
            self.give_up(Emit::NeedsEngine(Why::TailUnreachable), out);
        }
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(volume, range, out);
    }

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        self.member_extent(member, range, out);
    }

    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.rebuild_posted(volume, range, read, into)
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        if self.lengths.contains_key(&volume) {
            return;
        }
        self.lengths.insert(volume, length);
        self.stale = true;
        self.settle(out);
    }

    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        if self.gone
            || self
                .candidates
                .iter()
                .any(|candidate| candidate.secret.as_str() == secret)
        {
            return;
        }
        self.candidates.push(Candidate {
            secret: Zeroizing::new(secret.to_owned()),
            password: Password::new(secret),
            header_wrong: false,
            folder_wrong: false,
        });
        self.stale = true;
        self.settle(out);
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes> {
        if envelope != ENVELOPE {
            return None;
        }
        self.kept.read(range).map(bytes::Bytes::copy_from_slice)
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str) {
        self.names.insert(volume, name.to_owned());
    }

    fn claim(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        // A volume of a split set has no signature of its own; one that has
        // is a container of its own.
        if first_bytes.starts_with(&header::MAGIC) {
            return None;
        }
        let wanted = SevenZip::parse(name)?;
        let volume = VolumeIndex(wanted.index());
        let ours = self
            .names
            .values()
            .filter_map(|bound| SevenZip::parse(bound))
            .any(|bound| bound.same_set(&wanted));
        (ours && wanted.index() != 0 && !self.names.contains_key(&volume)).then_some(volume)
    }
}
