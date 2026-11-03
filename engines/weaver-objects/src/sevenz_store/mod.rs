// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The 7z store: a 7z container, one file, streamed into its members as its
// bytes arrive, in any order, with no engine and no file of its own. A 7z
// posted in numbered pieces is the split store's, which joins them into
// the one file this store reads. It reads what it needs and nothing else: the signature
// header, then the end header, which it bids for out of order because it
// sits at the container's tail, then the map the header gives. Stored
// folders map straight to members; AES folders map through the cipher once
// a password is proved. Anything that compresses goes to an engine.
//
// The object is pure: no I/O, no threads, no files. Bytes it cannot place yet
// are held by its owner, who hands them back once the map places them.

mod header;
mod map;
mod open;
mod place;
mod record;

#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sevenz_turbo::Password;
use weaver_graph::{
    Budget, ByteRange, Claim, Coverage, Delivery, Durable, Emit, Extent, Facts, Give, Handed, Kind,
    Located, MemberId, Object, Placement, Posting, Pull, Seek, Standing, Take, VolumeIndex, Weight,
    Why,
};
use zeroize::Zeroizing;

use crate::RECOVERY;
use crate::crypt::{Cbc, Chain, Padding};
use crate::grid::{Grid, crc32};
use crate::kept::Kept;
use crate::keys::Keys;
use crate::lost::minus;
use header::{SIGNATURE, Signature, Start};
use map::Map;

// The store's one volume: the file. Its coordinates are the container's.
const FILE: VolumeIndex = VolumeIndex(0);

// What a 7z decode within `budget` may hold: the memory granted, or the
// decoder's own limits when none was. Header parses, key derivations and
// runs alike are held to it.
pub(crate) fn limits(budget: &Budget) -> sevenz_turbo::ArchiveLimits {
    let limits = sevenz_turbo::ArchiveLimits::default();
    sevenz_turbo::ArchiveLimits {
        memory_limit_bytes: budget.memory_or(limits.memory_limit_bytes),
        ..limits
    }
}

// The length of the container a front's signature header states, when the
// front is one: a front that states what this file's front stated is this
// file posted again.
pub(crate) fn measured(first_bytes: &[u8]) -> Option<u64> {
    match header::signature(first_bytes) {
        Signature::Start(start) => Some(start.total()),
        _ => None,
    }
}

// The one envelope: container coordinates, for every byte that belongs to no
// member.
const ENVELOPE: u32 = 0;

// A folder's cipher, by what the object knows of its key. A sealed one was
// open before a restart: it keeps its chain and waits, as a locked one does,
// for the password that keys it again. An open or sealed folder keeps the
// plaintext its cipher stream carries past its files, which no member holds;
// what else came out of the cipher is its members' coverage. An open one is
// shared with the postings handed out of the store, and written on its own
// copy once one is out.
enum Lock {
    Plain,
    Locked,
    Open(Arc<Cbc>, Arc<Padding>),
    Sealed(Box<Chain>, Padding),
}

impl Lock {
    // A folder no key has opened yet.
    fn fresh(folder: &map::Folder) -> Self {
        match folder.aes {
            Some(_) => Lock::Locked,
            None => Lock::Plain,
        }
    }

    fn is_locked(&self) -> bool {
        matches!(self, Lock::Locked | Lock::Sealed(..))
    }

    // The plaintext past the files, of a cipher that was ever open.
    fn padding_mut(&mut self) -> Option<&mut Padding> {
        match self {
            Lock::Open(_, padding) => Some(Arc::make_mut(padding)),
            Lock::Sealed(_, padding) => Some(padding),
            Lock::Plain | Lock::Locked => None,
        }
    }
}

// A lock as its facts keep it: an open cipher is kept as its chain, never
// its key.
#[derive(Serialize, Deserialize)]
enum LockFacts {
    Plain,
    Locked,
    Sealed(Chain, Padding),
}

impl Serialize for Lock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Lock::Plain => LockFacts::Plain,
            Lock::Locked => LockFacts::Locked,
            Lock::Open(cbc, padding) => LockFacts::Sealed(cbc.chain(), Padding::clone(padding)),
            Lock::Sealed(chain, padding) => LockFacts::Sealed(Chain::clone(chain), padding.clone()),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Lock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match LockFacts::deserialize(deserializer)? {
            LockFacts::Plain => Lock::Plain,
            LockFacts::Locked => Lock::Locked,
            LockFacts::Sealed(chain, padding) => Lock::Sealed(Box::new(chain), padding),
        })
    }
}

// What has been said for one member.
#[derive(Clone, Default, Serialize, Deserialize)]
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

// Its facts are all of it but what keys it: no password, no candidate and
// no key. The owner offers the winning password again, and the folders open
// from the AES properties in the map; whether it had proved itself is kept,
// so it is proved still. Bids are not kept either; a restart makes them
// again.
#[derive(Serialize, Deserialize)]
pub struct SevenZipStore {
    #[serde(skip, default = "Keys::global")]
    keys: Arc<Keys>,
    // What its header parses and key derivations may hold, as granted when
    // it was built or restored.
    #[serde(skip)]
    budget: Budget,
    // How long the file is, once its parent says.
    length: Option<u64>,
    kept: Kept,
    start: Option<Start>,
    // How long the container is, as the signature header in the front the
    // file was bound with states it, before any byte is delivered: a front
    // that states it again is the file posted again.
    measured: Option<u64>,
    // The end header is known to be encrypted.
    encrypted: bool,
    // Container ranges the header parse needs, the end header among them.
    header_ranges: Vec<ByteRange>,
    // Shared, with what its members said, by the postings handed out.
    map: Option<Arc<Map>>,
    // The map a changed header gives, while the bytes it moves are asked
    // for again under the one that placed them; it takes over at the next
    // input.
    next: Option<Arc<Map>>,
    // The header bytes the map was built on, by range and checksum; none
    // once any of them changed, until a header is read again.
    basis: Vec<(ByteRange, u32)>,
    folders: Vec<Lock>,
    members: Arc<Vec<Out>>,
    grid: Grid,
    #[serde(skip)]
    candidates: Vec<Candidate>,
    // The candidate that opened the header or the folders.
    #[serde(skip)]
    opened: Option<usize>,
    // Whether the header or a proof file proved it. A candidate opened
    // unproven is on trial: a checksum it fails on verified bytes condemns
    // it. Kept: the candidate a restart offers again is the one that proved
    // itself, and a checksum it fails after is damage, as before.
    proven: bool,
    // A checksum failed on verified bytes under the candidate on trial.
    #[serde(skip)]
    wrong_key: bool,
    // Open folders keyed by a condemned candidate, until their bytes are
    // said again; then, until the next look, folders to lock again.
    #[serde(skip)]
    condemned: BTreeSet<usize>,
    #[serde(skip)]
    relock: BTreeSet<usize>,
    proof: Option<open::Proof>,
    asked: bool,
    // Container ranges wanted ahead of the normal order.
    bids: Vec<(ByteRange, Weight)>,
    damage_said: HashSet<(u64, u64, u32)>,
    recovery_said: bool,
    // Something changed since the object last looked.
    stale: bool,
    // The object gave its container up; it says nothing more.
    gone: bool,
}

impl Durable for SevenZipStore {
    // 3: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 4: a sealed value encodes as nothing, numbered by its owner's walk.
    // 5: the container's length as the first volume's front states it.
    // 6: whether the candidate that opened it had proved itself.
    // 7: the places whose volume's front came.
    // 8: the map a changed header gives, until it takes over.
    // 9: the places of 7 dropped again; the tree judges a held place by fronts.
    // 10: one file: the volumes' names, lengths and geometry dropped, since
    //     a 7z posted in numbered parts is the split store's to join.
    // 11: a kept cipher block says whether an arrival started inside it.
    const VERSION: u16 = 11;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.grid.parts(take)
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.grid.restore_parts(give)
    }

    // The folders and members said are the map's, a cipher only where the
    // map has AES, and the proof is a file of the map.
    fn impossible(&self) -> Option<&'static str> {
        let mapped =
            match &self.map {
                None => self.folders.is_empty() && self.members.is_empty() && self.proof.is_none(),
                Some(map) => {
                    map.is_sound()
                        && map.folders.len() == self.folders.len()
                        && map.entries.len() == self.members.len()
                        && self.folders.iter().zip(&map.folders).all(|(lock, folder)| {
                            matches!(lock, Lock::Plain) == folder.aes.is_none()
                        })
                        && self.proof.is_none_or(|proof| proof.fits(map))
                }
            };
        let next = self
            .next
            .as_ref()
            .is_none_or(|next| self.map.is_some() && next.is_sound());
        (!mapped || !next || !self.kept.sound()).then_some("a map no 7z header gives")
    }
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
            budget: Budget::default(),
            length: None,
            kept,
            start: None,
            measured: None,
            encrypted: false,
            header_ranges: Vec::new(),
            map: None,
            next: None,
            basis: Vec::new(),
            folders: Vec::new(),
            members: Arc::default(),
            grid: Grid::default(),
            candidates: Vec::new(),
            opened: None,
            proven: false,
            wrong_key: false,
            condemned: BTreeSet::new(),
            relock: BTreeSet::new(),
            proof: None,
            asked: false,
            bids: Vec::new(),
            damage_said: HashSet::new(),
            recovery_said: false,
            stale: true,
            gone: false,
        }
    }

    // Held to what `budget` grants.
    pub fn within(mut self, budget: Budget) -> Self {
        self.budget = budget;
        self
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    // What shuts the container once no candidate is left to try: its header,
    // when none opened it, or a folder none keys.
    fn shut(&self) -> Option<Why> {
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
        (header_shut || folders_shut).then_some(Why::EncryptedHeader)
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
        out.push(Emit::Damaged {
            volume: FILE,
            range,
        });
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
        self.check_recovery(start, out);
        if self.map.is_none() || self.basis.is_empty() {
            self.read_header(start, out);
        }
        for folder in std::mem::take(&mut self.relock) {
            self.folders[folder] = Lock::Locked;
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
            // A 7z this object cannot read is still a 7z: it is refused,
            // and stays the container it is.
            Signature::Unreadable => {
                self.give_up(Emit::Refuse(Why::Unsupported), out);
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

    // Recovery data appended after the container.
    fn check_recovery(&mut self, start: Start, out: &mut Vec<Emit>) {
        let Some(length) = self.length else {
            return;
        };
        if self.recovery_said {
            return;
        }
        let after = start.total();
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
                volume: FILE,
                range: ByteRange::new(after, length),
                kind: *kind,
                handed: Handed::default(),
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
            match header::parse(&self.kept, total, None, false, &limits(&self.budget)) {
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
            Err(why) => return self.give_up(Emit::NeedsEngine(why), out),
        };
        self.basis = read
            .iter()
            .filter_map(|range| self.kept.read(*range).map(|bytes| (*range, crc32(bytes))))
            .collect();
        match self.map.clone() {
            None => self.adopt(map, out),
            Some(old) => self.remap(&old, map, out),
        }
    }

    // The first map: every member is announced now, in id order, which is
    // the order the namespace numbers them in.
    fn adopt(&mut self, map: Map, out: &mut Vec<Emit>) {
        self.folders = map.folders.iter().map(Lock::fresh).collect();
        let mut members: Vec<Out> = map.entries.iter().map(|_| Out::default()).collect();
        for (id, member) in members.iter_mut().enumerate() {
            announce(&map, id, member, out);
        }
        self.members = Arc::new(members);
        self.map = Some(Arc::new(map));
        self.choose_proof();
    }

    // A password is proved against a file before it opens the folders.
    fn choose_proof(&mut self) {
        self.proof = None;
        let Some(map) = &self.map else {
            return;
        };
        if self.opened.is_none() && self.folders.iter().any(Lock::is_locked) {
            self.proof = open::Proof::choose(map);
            if let Some(proof) = &self.proof {
                let range = proof.range;
                self.kept.want(range);
                self.bid(range, Weight(1));
            }
        }
    }

    // A changed header over a map in hand. The members it places as the
    // old map did keep what they said; every other one moves. The bytes the
    // old map placed in a moving member, and those the new map places in
    // one, are asked for again: they read back through the old map, which
    // stands until the next input, and arrive under the new one.
    fn remap(&mut self, old: &Map, new: Map, out: &mut Vec<Emit>) {
        let mut extents = Vec::new();
        for id in moved(old, &new) {
            let member = MemberId(id as u32);
            self.extent_in(old, member, ByteRange::new(0, u64::MAX), &mut extents);
            self.extent_in(&new, member, ByteRange::new(0, u64::MAX), &mut extents);
        }
        self.next = Some(Arc::new(new));
        if extents.is_empty() {
            return self.install(out);
        }
        extents.sort_by_key(|extent| extent.range.start);
        let mut ranges: Vec<ByteRange> = Vec::new();
        for Extent { range, .. } in extents {
            match ranges.last_mut() {
                Some(merged) if merged.end >= range.start => {
                    merged.end = merged.end.max(range.end);
                }
                _ => ranges.push(range),
            }
        }
        out.extend(ranges.into_iter().map(|range| Emit::Again {
            volume: FILE,
            range,
        }));
    }

    // The map a changed header gave takes over: each moving member starts
    // over under it, every other member and every folder it keeps stand as
    // they were, and the old map is dropped.
    fn install(&mut self, out: &mut Vec<Emit>) {
        let Some(new) = self.next.take() else {
            return;
        };
        let Some(old) = self.map.take() else {
            return;
        };
        let moved = moved(&old, &new);
        for &id in moved.iter().filter(|id| **id < old.entries.len()) {
            out.push(Emit::Retract(MemberId(id as u32)));
            self.grid.forget(MemberId(id as u32));
        }
        // A folder the new map keeps keeps its cipher; the others start
        // locked or plain.
        let was: Vec<Option<usize>> = new
            .folders
            .iter()
            .map(|folder| {
                old.folders
                    .binary_search_by_key(&folder.pack.start, |old| old.pack.start)
                    .ok()
                    .filter(|at| same_folder(&old.folders[*at], folder))
            })
            .collect();
        let mut locks = std::mem::take(&mut self.folders);
        self.folders = new
            .folders
            .iter()
            .zip(&was)
            .map(|(folder, was)| match was {
                Some(at) => std::mem::replace(&mut locks[*at], Lock::Locked),
                None => Lock::fresh(folder),
            })
            .collect();
        let carried = |set: &BTreeSet<usize>| -> BTreeSet<usize> {
            was.iter()
                .enumerate()
                .filter_map(|(at, was)| was.filter(|was| set.contains(was)).map(|_| at))
                .collect()
        };
        self.condemned = carried(&self.condemned);
        self.relock = carried(&self.relock);
        let mut members: Vec<Out> = (0..new.entries.len())
            .map(|id| match moved.binary_search(&id) {
                Ok(_) => Out::default(),
                Err(_) => self.members[id].clone(),
            })
            .collect();
        for &id in moved.iter().filter(|id| **id < new.entries.len()) {
            announce(&new, id, &mut members[id], out);
        }
        self.members = Arc::new(members);
        self.map = Some(new);
        if self.proof.is_some() || self.opened.is_none() {
            self.choose_proof();
        }
        self.stale = true;
    }

    // Header bytes the map stands on changed. The map stands until a
    // header is read again; a changed signature header names another end
    // header, so it is read again too.
    fn rebuild_if_changed(&mut self, range: ByteRange) {
        if self.map.is_none() {
            return;
        }
        let footing = match self.basis.is_empty() {
            true => SIGNATURE.intersect(range).is_some(),
            false => self
                .basis
                .iter()
                .any(|(basis, _)| basis.intersect(range).is_some()),
        };
        let same = !self.basis.is_empty()
            && self.basis.iter().all(|(basis, crc)| {
                self.kept
                    .read(*basis)
                    .is_some_and(|bytes| crc32(bytes) == *crc)
            });
        if footing && !same {
            self.basis.clear();
            self.start = None;
        }
    }
}

// Announce a member of a map, as its namespace numbers it.
fn announce(map: &Map, id: usize, said: &mut Out, out: &mut Vec<Emit>) {
    let entry = &map.entries[id];
    let member = MemberId(id as u32);
    out.push(Emit::Member {
        member,
        name: entry.name.clone(),
        length: Some(entry.length),
    });
    // A 7z checks its members' plain bytes, keyed or not.
    if let Some(crc32) = entry.crc {
        out.push(Emit::Checksum { member, crc32 });
    }
    if entry.length == 0 {
        said.closed = true;
        out.push(Emit::MemberClosed(member));
    }
}

// The members two maps place differently, by id, ascending: another name,
// length or checksum, or other bytes of the container.
fn moved(old: &Map, new: &Map) -> Vec<usize> {
    (0..old.entries.len().max(new.entries.len()))
        .filter(|&id| match (old.entries.get(id), new.entries.get(id)) {
            (Some(was), Some(is)) => {
                was.name != is.name
                    || was.length != is.length
                    || was.crc != is.crc
                    || was.offset != is.offset
                    || match (was.folder, is.folder) {
                        (None, None) => false,
                        (Some(a), Some(b)) => !same_folder(&old.folders[a], &new.folders[b]),
                        _ => true,
                    }
            }
            _ => true,
        })
        .collect()
}

// One folder in both maps: the same pack, plaintext and cipher.
fn same_folder(a: &map::Folder, b: &map::Folder) -> bool {
    a.pack == b.pack
        && a.plain_len == b.plain_len
        && match (&a.aes, &b.aes) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                a.cycles_power() == b.cycles_power() && a.salt() == b.salt() && a.iv() == b.iv()
            }
            _ => false,
        }
}

impl Object for SevenZipStore {
    fn kind(&self) -> Kind {
        Kind::SevenZipStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        self.install(out);
        let range = delivery.range();
        if range.is_empty() {
            return;
        }
        let bytes = delivery.bytes;
        // The bytes of a proof still pending are kept, and reach their members
        // only when the proof opens its folder, in this call or a later one.
        let proof = self.proof.map(|proof| proof.range);
        // Absorbing can make the object want more of this very delivery: the
        // signature names the end header, the end header names its packs.
        loop {
            let generation = self.kept.generation();
            if self.kept.absorb(range, bytes, delivery.provenance) {
                self.stale = true;
                self.rebuild_if_changed(range);
            }
            self.settle(out);
            if self.gone {
                return;
            }
            if self.kept.generation() == generation {
                break;
            }
        }
        let pieces: Vec<ByteRange> = match proof {
            Some(proof) => minus(range, proof).collect(),
            None => vec![range],
        };
        for piece in pieces {
            let at = (piece.start - range.start) as usize;
            let slice = bytes.slice(at..at + piece.len() as usize);
            self.route(piece, &slice, delivery.provenance, read, out);
        }
        if std::mem::take(&mut self.wrong_key) {
            self.condemn(out);
        }
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, out: &mut Vec<Emit>) {
        self.install(out);
        self.stale = true;
        self.settle(out);
    }

    // Nothing more will arrive. A member still damaged, a password that
    // opened nothing, or a member named and never placed leaves the
    // container unread.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        self.install(out);
        let why = crate::quiet::damaged(standing)
            .or_else(|| self.shut())
            .or_else(|| crate::quiet::unplaced(standing));
        if let Some(why) = why {
            self.give_up(Emit::Refuse(why), out);
        }
    }

    fn volume_lost(&mut self, _volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        self.install(out);
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

    fn wants(&self, out: &mut Vec<Seek>) {
        // Nothing is bid before the file's length is known: where its tail
        // lies, and which bytes are past it, follow from that length.
        let Some(length) = self.length else {
            return;
        };
        let file = ByteRange::new(0, length);
        for (range, weight) in &self.bids {
            // A range in hand is wanted no more, nor bytes past the file.
            if self.kept.read(*range).is_none()
                && let Some(range) = range.intersect(file)
            {
                out.push(Seek::data(FILE, range, *weight));
            }
        }
    }

    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        self.install(out);
        if pull != Pull::Unseekable || self.map.is_some() {
            return;
        }
        // The header is at the tail, and nothing above can bring the tail
        // forward: everything ahead of it would have to be held.
        if self
            .header_ranges
            .iter()
            .any(|header| header.intersect(seek.range).is_some())
        {
            self.give_up(Emit::NeedsEngine(Why::TailUnreachable), out);
        }
    }

    fn locate(&self, _volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(range, out);
    }

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        self.member_extent(member, range, out)
    }

    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.posted_over(&[(volume, range)])
            .posted(volume, range, read, into)
    }

    fn posting(&self, ranges: &[(VolumeIndex, ByteRange)]) -> Box<dyn Posting> {
        Box::new(self.posted_over(ranges))
    }

    fn unposted(&mut self, _volume: VolumeIndex, range: ByteRange) {
        self.unsay(range);
    }

    fn sized(&mut self, _volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        self.install(out);
        if self.gone || self.length == Some(length) {
            return;
        }
        self.length = Some(length);
        self.stale = true;
        self.settle(out);
    }

    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        // The offer answers the question the store asked: a candidate that
        // opens nothing leaves it free to ask for the next one.
        self.install(out);
        self.asked = false;
        if !self
            .candidates
            .iter()
            .any(|candidate| candidate.secret.as_str() == secret)
        {
            self.candidates.push(Candidate {
                secret: Zeroizing::new(secret.to_owned()),
                password: Password::new(secret),
                header_wrong: false,
                folder_wrong: false,
            });
        }
        self.stale = true;
        self.settle(out);
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes> {
        if envelope != ENVELOPE {
            return None;
        }
        self.kept.read(range).map(bytes::Bytes::copy_from_slice)
    }

    // The file's front says how long the container is, which says whether
    // a front offered later is this file posted again.
    fn bound(&mut self, _volume: VolumeIndex, _name: &str, front: &[u8], length: Option<u64>) {
        if let Some(length) = length {
            self.length.get_or_insert(length);
            self.stale = true;
        }
        if let Some(total) = measured(front) {
            self.measured = Some(total);
        }
    }

    fn engine_volume(&self, _volume: VolumeIndex) -> Option<weaver_graph::Carried> {
        Some(Box::new(crate::engine::CarriedVolume {
            front: self.measured,
            length: self.length,
        }))
    }

    fn refused(&mut self) {
        self.gone = true;
    }

    // A 7z is one file: no name or length makes another member a volume of
    // it. A front whose signature states the container this file's stated
    // is this file posted again, offered its place for the tree to weigh
    // against what holds it, by their fronts.
    fn claim(&mut self, _name: &str, first_bytes: &[u8], _length: Option<u64>) -> Claim {
        match measured(first_bytes) {
            Some(total) if Some(total) == self.measured => vec![FILE].into(),
            _ => Claim::default(),
        }
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::SevenZipStore, self)
    }
}
