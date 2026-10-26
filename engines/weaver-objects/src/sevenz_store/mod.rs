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
mod map;
mod open;
mod place;
mod record;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sevenz_turbo::Password;
use weaver_graph::{
    ByteRange, Coverage, Delivery, Durable, Emit, Extent, Facts, Give, Handed, Kind, MemberId,
    Object, Placement, Posting, Pull, Seek, Sniff, Standing, Take, VolumeIndex, Weight, Why,
    signed,
};
use zeroize::Zeroizing;

use crate::RECOVERY;
use crate::crypt::{Cbc, Chain, Padding};
use crate::grid::{Grid, crc32};
use crate::kept::Kept;
use crate::keys::Keys;
use crate::quiet::Placed;
use crate::split;
use geometry::Geometry;
use header::{SIGNATURE, Signature, Start};
use map::Map;

// The length of the container a first piece's signature header measures,
// when the front is one: a front that measures what the set's first piece
// measured is that piece.
pub(crate) fn measured(first_bytes: &[u8]) -> Option<u64> {
    match header::signature(first_bytes) {
        Signature::Start(start) => Some(start.total()),
        _ => None,
    }
}

// Where a piece with no number goes by its length alone, among the free
// places of a set whose first piece is `first` bytes long and measures a
// container of `total`: every piece but the last is as long as the first,
// so a piece that long fits every free place but the last.
pub(crate) fn fits(
    first: u64,
    total: u64,
    length: u64,
    free: impl Fn(VolumeIndex) -> bool,
) -> Option<Placed> {
    let geometry = Geometry::new(first, total)?;
    Placed::of(
        (1..=geometry.last().0)
            .map(VolumeIndex)
            .filter(|at| free(*at) && geometry.holds(*at) == length),
    )
}

// The one envelope: container coordinates, for every byte that belongs to no
// member. Bytes a volume carries past what the geometry gives it go to the
// envelope after it, one per volume, which holds nothing.
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
// from the AES properties in the map. Bids are not kept either; a restart
// makes them again.
#[derive(Serialize, Deserialize)]
pub struct SevenZipStore {
    #[serde(skip, default = "Keys::global")]
    keys: Arc<Keys>,
    names: BTreeMap<VolumeIndex, String>,
    lengths: BTreeMap<VolumeIndex, u64>,
    kept: Kept,
    start: Option<Start>,
    // How long the container is, as the signature header in the front the
    // first piece was bound with states it: with that piece's length, what
    // places the pieces bound after it, before any byte is delivered.
    measured: Option<u64>,
    geometry: Option<Geometry>,
    // The end header is known to be encrypted.
    encrypted: bool,
    // Container ranges the header parse needs, the end header among them.
    header_ranges: Vec<ByteRange>,
    // Shared, with what its members said, by the postings handed out.
    map: Option<Arc<Map>>,
    // The header bytes the map was built on, by range and checksum.
    basis: Vec<(ByteRange, u32)>,
    folders: Vec<Lock>,
    members: Arc<Vec<Out>>,
    grid: Grid,
    #[serde(skip)]
    candidates: Vec<Candidate>,
    // The candidate that opened the header or the folders.
    #[serde(skip)]
    opened: Option<usize>,
    // Whether a proof file proved it. A candidate opened unproven is on
    // trial: a checksum it fails on verified bytes condemns it. Not kept: a
    // restart offers the candidate again and admits it unproven, and a
    // candidate it condemns then costs one more reading of its bytes.
    #[serde(skip)]
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
    lengths_said: BTreeSet<VolumeIndex>,
    recovery_said: bool,
    // Something changed since the object last looked.
    stale: bool,
    // The object gave its container up; it says nothing more.
    gone: bool,
}

impl Durable for SevenZipStore {
    // 3: the grid shelved by chunk of blocks, kept apart from the bytes.
    // 4: a sealed value encodes as nothing, numbered by its owner's walk.
    // 5: the container's length as the first piece's front states it.
    const VERSION: u16 = 5;

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
        (!mapped || !self.kept.sound()).then_some("a map no 7z header gives")
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
            names: BTreeMap::new(),
            lengths: BTreeMap::new(),
            kept,
            start: None,
            measured: None,
            geometry: None,
            encrypted: false,
            header_ranges: Vec::new(),
            map: None,
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

    // Where a member's facts put it in the set. A piece of a split set has
    // no signature of its own; one whose front a format names is that
    // format's, a container of its own or a recovery volume, however long
    // it is. The first piece is the one signed piece: a front whose
    // signature measures the container the first piece measured is that
    // piece posted again. A piece numbered from the first piece's name is
    // at that number; one with no number is at each free place its length
    // fits, and a piece as long as the first fits every free place but
    // the last.
    fn placed(&self, name: &str, first_bytes: &[u8], length: Option<u64>) -> Option<Placed> {
        if matches!(signed(first_bytes, length), Sniff::Object(_)) {
            let again = matches!(
                header::signature(first_bytes),
                Signature::Start(start) if Some(start.total()) == self.measured
            );
            return again.then_some(Placed::At(VolumeIndex(0)));
        }
        let first = *self.lengths.get(&VolumeIndex(0))?;
        let (total, length, names) = (self.measured?, length?, &self.names);
        Geometry::new(first, total)?;
        let numbered = split::parse(names.get(&VolumeIndex(0))?)
            .and_then(|first| split::parse(name)?.index_from(&first))
            .map(VolumeIndex);
        if let Some(numbered) = numbered {
            return Some(Placed::At(numbered));
        }
        fits(first, total, length, |at| !names.contains_key(&at))
    }

    // One past every volume held.
    fn past(&self) -> Option<VolumeIndex> {
        Placed::past(self.names.keys().next_back().map(|volume| volume.0))
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
            out.push(Emit::Damaged {
                volume: *volume,
                range: ByteRange::new(0, (*length).max(geometry.holds(*volume))),
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
            Err(why) => return self.give_up(Emit::NeedsEngine(why), out),
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
        let mut members: Vec<Out> = map.entries.iter().map(|_| Out::default()).collect();
        // Every member is announced now, in id order, which is the order the
        // namespace numbers them in.
        for (id, entry) in map.entries.iter().enumerate() {
            let member = MemberId(id as u32);
            out.push(Emit::Member {
                member,
                name: entry.name.clone(),
                length: Some(entry.length),
            });
            if entry.length == 0 {
                members[id].closed = true;
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
        self.members = Arc::new(members);
        self.map = Some(Arc::new(map));
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
        self.members = Arc::default();
        self.grid = Grid::default();
        self.proof = None;
        self.condemned.clear();
        self.relock.clear();
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
        self.route(range, bytes, delivery.provenance, read, out);
        if std::mem::take(&mut self.wrong_key) {
            self.condemn(out);
        }
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, out: &mut Vec<Emit>) {
        self.stale = true;
        self.settle(out);
    }

    // Nothing more will arrive. A member still damaged, a password that
    // opened nothing, a piece beside the set that nothing orders, or a
    // member named and never placed leaves the container unread.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        let why = crate::quiet::damaged(standing)
            .or_else(|| self.shut())
            .or_else(|| {
                crate::quiet::unordered(standing, |beside| {
                    self.placed(beside.name, beside.front, beside.length)
                })
            })
            .or_else(|| crate::quiet::unplaced(standing));
        if let Some(why) = why {
            self.give_up(Emit::Refuse(why), out);
        }
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
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

    fn wants(&self, out: &mut Vec<Seek>) {
        // Only volumes bound so far can be bid: a seek into one not yet
        // claimed would be answered as unseekable.
        if self.geometry.is_none() {
            return;
        }
        let mut pieces = Vec::new();
        for (range, weight) in &self.bids {
            // A range in hand is wanted no more.
            if self.kept.read(*range).is_some() {
                continue;
            }
            pieces.clear();
            self.to_volumes(*range, &mut pieces);
            for (volume, range) in &pieces {
                if self.names.contains_key(volume) {
                    out.push(Seek::data(*volume, *range, *weight));
                }
            }
        }
    }

    fn pulled(&mut self, seek: Seek, pull: Pull, out: &mut Vec<Emit>) {
        if pull != Pull::Unseekable || self.map.is_some() {
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
        self.posted_over(&[(volume, range)])
            .posted(volume, range, read, into)
    }

    fn posting(&self, ranges: &[(VolumeIndex, ByteRange)]) -> Box<dyn Posting> {
        Box::new(self.posted_over(ranges))
    }

    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        self.unsay(volume, range);
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
        // The offer answers the question the store asked: a candidate that
        // opens nothing leaves it free to ask for the next one.
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

    // The first volume is the piece with the signature header, whose front
    // says how long the container is; its length is how long every piece
    // but the last is. Both are what places the pieces bound after it.
    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], length: Option<u64>) {
        self.names.insert(volume, name.to_owned());
        if let Some(length) = length {
            self.lengths.entry(volume).or_insert(length);
            self.stale = true;
        }
        if volume.0 == 0
            && let Signature::Start(start) = header::signature(front)
        {
            self.measured = Some(start.total());
        }
    }

    fn renamed(&mut self, volume: VolumeIndex, name: &str) {
        self.names.insert(volume, name.to_owned());
    }

    fn refused(&mut self) {
        self.gone = true;
    }

    // Every piece but the last is as long as the first, and the last holds
    // the rest of the container the first piece's signature header
    // measures, so a piece's length says which free places it can take.
    // Where it could take several, as a piece as long as the first can,
    // only the number its name carries, counted from the first piece's own,
    // can say which: a numbered piece takes that place whatever its length,
    // and whether or not it is held, since a piece posted again is that
    // piece; the store checks its length as it checks every volume's. A
    // piece with no number takes the one free place its length fits
    // exactly, and a piece no fact places is not claimed. One that fits
    // more than one free place is the set's and nothing orders it: a name a
    // recovery set gives it later may, so it is not claimed while the set
    // stands, and the set refuses on quiet for it.
    fn claim(
        &mut self,
        name: &str,
        first_bytes: &[u8],
        length: Option<u64>,
    ) -> Option<VolumeIndex> {
        self.placed(name, first_bytes, length)?
            .claimed(self.gone, self.past())
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::SevenZipStore, self)
    }
}
