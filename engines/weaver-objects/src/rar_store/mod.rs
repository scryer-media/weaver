// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR store: a RAR4 or RAR5 set, one volume or many, streamed into its
// members as its bytes arrive, in any order, with no engine and no file of
// its own. Every volume's headers are walked by unrar-rs from its front, so
// nothing is ever wanted from a tail, and unrar-rs's stored layout links
// them into members: a stored file maps straight to its member, piece by
// piece across the volumes it spans; an AES file maps through the cipher
// once a password opens it. Anything compressed or solid goes to an engine.
//
// The object is pure: no I/O, no threads, no files. Bytes it cannot place yet
// are held by its owner, who hands them back once the headers place them.

pub(crate) mod claim;
pub(crate) mod facts;
mod gaps;
mod image;
mod layout;
mod lock;
mod place;
mod record;
mod walk;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet, btree_map};
use std::ops::Bound;
use std::sync::Arc;

use unrar_rs::{ArchiveFormat, EncryptedStore};
use weaver_graph::{
    ByteRange, Claim, Coverage, Delivery, Emit, Extent, Kind, Located, MemberId, Object, Placement,
    Posting, Provenance, Pull, Seek, Standing, VolumeIndex, Weight, Why,
};
use zeroize::Zeroizing;

use crate::crypt::{Cbc, Chain, Padding};
use crate::engine::SetName;
use crate::grid::crc32;
use crate::kept::Kept;
use crate::keys::Keys;
use crate::placed::Placed;
use crate::rar_engine::naming;
use crate::runs::CrcRuns;
use walk::{Step, Walk};

// How many volumes past the one being read are bid for. A solid set never
// streams here, so only the non-solid window applies.
const WINDOW: u32 = 8;

// One volume as the store holds it: everything it read of it, the headers
// walked and the bytes kept for that, which is what travels with the member
// when the store gives the volume back.
pub(crate) struct Volume {
    name: Option<String>,
    // Where the name puts the volume in its set's numbering, read once as
    // the name is given.
    numbered: Option<naming::VolumeName>,
    // The name it was first given, as posted, and where that puts it: a
    // recovery set may rename it, and its siblings still posted under the
    // same numbering are this set's all the same.
    posted: Option<String>,
    posted_numbered: Option<naming::VolumeName>,
    length: Option<u64>,
    closed: bool,
    kept: Kept,
    walk: Walk,
    // What the volume's front stated when it was bound, before any of its
    // bytes reached the walk: what places a sibling beside it meanwhile.
    front: Option<unrar_rs::RarVolumeFacts>,
    // Cleared at touch, before a walk, and when a damaged delivery wants
    // more header bytes. Payload-only arrivals reuse the encoded metadata.
    facts: std::sync::Mutex<Option<weaver_graph::Sealed<facts::VolumeFacts>>>,
}

impl Volume {
    fn new() -> Self {
        Self {
            name: None,
            numbered: None,
            posted: None,
            posted_numbered: None,
            length: None,
            closed: false,
            kept: Kept::default(),
            walk: Walk::default(),
            front: None,
            facts: std::sync::Mutex::new(None),
        }
    }

    fn invalidate_facts(&mut self) {
        *self
            .facts
            .get_mut()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }

    // What the volume's headers have stated so far: what its walk read once
    // that says whether it is one of a set, before then what its front
    // stated. A walk stopped short of the main header, as one at enciphered
    // headers with no password is, has stated nothing yet.
    fn stated(&self) -> Option<&unrar_rs::RarVolumeFacts> {
        self.walk
            .facts
            .as_ref()
            .filter(|_| self.walk.in_set().is_some())
            .or(self.front.as_ref())
    }

    // Whether what the volume's headers have stated says it is not at
    // `index`. The one judgement of a volume's place: where the store
    // claims a volume, holds one, and gives one back, so it never claims a
    // volume where it would give it back.
    fn misplaced(&self, index: VolumeIndex) -> bool {
        self.stated()
            .is_some_and(|facts| claim::misplaced(index, facts))
    }

    fn name_as(&mut self, name: &str) {
        self.numbered = numbering(name);
        self.name = Some(name.to_owned());
        if self.posted.is_none() {
            self.posted = Some(name.to_owned());
            self.posted_numbered = self.numbered.clone();
        }
    }

    // Where each name the volume carried puts it: the one it has now and
    // the one it was posted under.
    fn numberings(&self) -> impl Iterator<Item = &naming::VolumeName> {
        self.numbered.iter().chain(self.posted_numbered.iter())
    }

    fn posted_as(&mut self, name: &str) {
        self.posted_numbered = numbering(name);
        self.posted = Some(name.to_owned());
    }
}

// A name's place in its set's numbering.
fn numbering(name: &str) -> Option<naming::VolumeName> {
    #[cfg(test)]
    counts::bump(|counts| counts.names += 1);
    naming::parse(name)
}

// Where the volumes read in a format are filed.
fn shelf(format: ArchiveFormat) -> usize {
    match format {
        ArchiveFormat::Rar14 => 0,
        ArchiveFormat::Rar4 => 1,
        ArchiveFormat::Rar5 => 2,
    }
}

// What a test counts of the store's work, on its own thread: the volumes
// walked and how many walks, layouts begun from the first volume, and bound
// names read for their numbering.
#[cfg(test)]
mod counts {
    use std::cell::RefCell;
    use std::collections::BTreeSet;

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Counts {
        pub(crate) walked: BTreeSet<u32>,
        pub(crate) walks: usize,
        pub(crate) layouts: usize,
        pub(crate) names: usize,
        // Volumes the layout was asked to take, each with the stamp of the
        // facts it was asked on, and how often it was not asked since its
        // answer was known.
        pub(crate) asked: Vec<(u32, u64)>,
        pub(crate) known: usize,
    }

    thread_local! {
        static COUNTS: RefCell<Counts> = RefCell::default();
    }

    pub(crate) fn bump(count: impl FnOnce(&mut Counts)) {
        COUNTS.with(|cell| count(&mut cell.borrow_mut()));
    }

    // What was counted since the last take.
    pub(crate) fn take() -> Counts {
        COUNTS.with(RefCell::take)
    }
}

// A file's cipher, by what the object knows of its key. A sealed one was
// open before a restart: it keeps the cipher blocks it held, which are on
// no disk, and waits, as a locked one does, for the password that keys it
// again. An open one is shared with the postings handed out of the store,
// and written on its own copy once one is out.
enum Lock {
    Plain,
    Locked,
    Open(Arc<Cbc>),
    Sealed(Box<Chain>),
}

// One volume's part of a file: its data area there and where it starts in
// the file's packed stream.
struct Piece {
    volume: VolumeIndex,
    data: ByteRange,
    at: u64,
    // The CRC of this part's packed bytes, which every part but the last
    // carries in place of the file's, and whether it is keyed.
    crc: Option<u32>,
    uses_mac: bool,
    after: bool,
}

// What has been said for one member: what of it was placed, which an open
// cipher reads as what came out of it, and the plaintext its cipher stream
// carries past it, which no member holds. Shared, as an open cipher is.
#[derive(Clone, Default)]
struct Out {
    coverage: Coverage,
    closed: bool,
    padding: Padding,
}

struct Entry {
    // The name as the headers spell it, which every part repeats.
    name: String,
    length: Option<u64>,
    crc: Option<u32>,
    uses_mac: bool,
    store: Option<EncryptedStore>,
    pieces: Vec<Piece>,
    complete: bool,
    lock: Lock,
    hash_key: Option<Zeroizing<[u8; 32]>>,
    // The candidate that keyed the file when nothing proved it: on trial,
    // a whole-file checksum that fails on verified bytes condemns it.
    trial: Option<usize>,
    // What the file's bytes were when a checksum condemned the candidate on
    // trial over them, by offset: bytes a repair rewrites since were
    // posted wrong, and the checksum that condemned it may have failed on
    // them and not on a wrong key.
    condemned_on: Coverage,
    out: Arc<Out>,
    // The plaintext's runs for the whole-file CRC, and every part's packed
    // runs, by piece, for its own.
    plain_runs: CrcRuns,
    parts: BTreeMap<usize, CrcRuns>,
}

impl Entry {
    // The part of the file whose packed stream holds all of `stream`: a
    // range the entry has not taken a part for yet is no member's bytes.
    fn piece_of(&self, stream: ByteRange) -> Option<usize> {
        let piece = self
            .pieces
            .partition_point(|piece| piece.at <= stream.start)
            .checked_sub(1)?;
        let found = &self.pieces[piece];
        (stream.end <= found.at + found.data.len()).then_some(piece)
    }

    fn packed(&self) -> u64 {
        self.pieces.iter().map(|piece| piece.data.len()).sum()
    }

    // The member's length: what the header said, or, for a size it left
    // unknown, what the parts add up to once the last is in.
    fn plain_len(&self) -> Option<u64> {
        self.length.or_else(|| self.complete.then(|| self.packed()))
    }

    // Whether the store is the one to judge the member's whole checksum: a
    // keyed one is not the member's CRC32, so it is stated to nothing below,
    // which checks the CRC32 it is stated.
    fn judges_its_checksum(&self) -> bool {
        self.uses_mac
    }

    // Whether a repair rewriting `stream` rewrites bytes a candidate was
    // condemned over as they were posted.
    fn rewrites_condemned(&self, stream: ByteRange) -> bool {
        self.condemned_on
            .overlapping(stream)
            .any(|run| run.provenance != Provenance::Repaired)
    }
}

// A file a condemned candidate keyed, as it stood: what reads its parts
// back as they were posted, and the parts not yet posted again.
struct Condemned {
    cbc: Arc<Cbc>,
    out: Arc<Out>,
    parts: BTreeSet<VolumeIndex>,
}

// A password offered.
struct Candidate {
    secret: Zeroizing<String>,
    headers_wrong: bool,
    // A file it keyed on trial failed its checksum on verified bytes. Not
    // kept: a restart offers the candidate again and keys it on trial, and
    // condemning it once more costs one more reading of its bytes.
    files_wrong: bool,
}

pub struct RarStore {
    // The process's derivations, shared by every walk, check and key.
    keys: Arc<Keys>,
    volumes: BTreeMap<VolumeIndex, Volume>,
    // Volumes said to be given back, until the tree takes them.
    giving: BTreeMap<VolumeIndex, Volume>,
    // The volumes held, here or in `giving`, that no entry's piece lies in:
    // the ones that can travel to another owner. Filed where a volume comes
    // or goes and where pieces do, so no question about one scans pieces.
    unmapped: BTreeSet<VolumeIndex>,
    // The volumes held that are bound and not yet closed: more of them is
    // still to come. Filed where a volume is named, closes, comes or goes,
    // so asking whether everything bound has closed scans no volume.
    open: BTreeSet<VolumeIndex>,
    // Every entry's pieces by the volume they lie in, as (entry, piece),
    // filed where `unmapped` is, so finding a volume's parts scans no other
    // volume's pieces.
    pieces_in: BTreeMap<VolumeIndex, Vec<(usize, usize)>>,
    // The volumes to look at again: walk their headers, hold them to the
    // layout, say their recovery records. Filed wherever something a walk
    // or the layout reads of a volume changes, so a look visits only them.
    dirty: BTreeSet<VolumeIndex>,
    // What the walks of the volumes held say of the set: the volumes whose
    // headers say no volume follows them, and the volumes whose headers
    // were read in each format. Filed as a volume is walked, comes or goes.
    lasts: BTreeSet<VolumeIndex>,
    formats: [BTreeSet<VolumeIndex>; 3],
    // The layout kept: extended past its tail as volumes are read, laid
    // afresh only where a volume it holds changed what it said.
    layout: Option<layout::Laid>,
    entries: Vec<Entry>,
    candidates: Vec<Candidate>,
    // The candidate that opened the headers.
    headers_open: Option<usize>,
    asked: bool,
    // The candidate on trial a checksum condemned during this delivery.
    wrong_key: Option<usize>,
    // A repair rewrote bytes of a file no candidate keys that a candidate
    // was condemned over as they were posted, since the job was last quiet.
    rewritten: bool,
    // Files a condemned candidate keyed, with the cipher and record they
    // stood under, until every part of them has been posted again: their
    // bytes read back as they were posted.
    condemned: BTreeMap<usize, Condemned>,
    damage_said: HashSet<(VolumeIndex, u64, u64, u32)>,
    recovery_said: BTreeSet<(VolumeIndex, u64)>,
    gaps_said: gaps::Gaps,
    // The files may open now: a password, a cipher or a condemned key
    // changed.
    stale: bool,
    // The object gave its set up; it says nothing more.
    gone: bool,
    // Moved wherever anything `place` reads may change: a look at what
    // changed, a volume touched, a condemned file's cipher taken or let go.
    // Data that only passes through moves nothing.
    placed: Placed,
    // The keys a file needs of a candidate that the cache holds no answer
    // for, as (candidate, entry): derived away from the loop while the
    // store is lent out, and the files open once it is back.
    derive_due: BTreeSet<(usize, usize)>,
    // What the pool derived for them, until the files take it: the
    // process's cache is shared and may drop an entry before then.
    derived: BTreeMap<(usize, usize), lock::Derived>,
}

impl Default for RarStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RarStore {
    pub fn new() -> Self {
        Self::with_keys(Keys::global())
    }

    // With a key cache of its own, so a test counts its own derivations.
    pub fn with_keys(keys: Arc<Keys>) -> Self {
        Self {
            keys,
            volumes: BTreeMap::new(),
            giving: BTreeMap::new(),
            unmapped: BTreeSet::new(),
            open: BTreeSet::new(),
            pieces_in: BTreeMap::new(),
            dirty: BTreeSet::new(),
            lasts: BTreeSet::new(),
            formats: Default::default(),
            layout: None,
            entries: Vec::new(),
            candidates: Vec::new(),
            headers_open: None,
            asked: false,
            wrong_key: None,
            rewritten: false,
            condemned: BTreeMap::new(),
            damage_said: HashSet::new(),
            recovery_said: BTreeSet::new(),
            gaps_said: gaps::Gaps::default(),
            stale: false,
            gone: false,
            placed: Placed::default(),
            derive_due: BTreeSet::new(),
            derived: BTreeMap::new(),
        }
    }

    // Something `place` reads may have changed.
    fn moved(&mut self) {
        self.placed.moved();
    }

    // A volume newly held lies under no piece: a layout maps only volumes
    // held, and one leaves only once nothing maps it. Its headers are walked
    // at the next look.
    fn volume(&mut self, volume: VolumeIndex) -> &mut Volume {
        match self.volumes.entry(volume) {
            btree_map::Entry::Occupied(found) => found.into_mut(),
            btree_map::Entry::Vacant(place) => {
                if !self.giving.contains_key(&volume) {
                    self.unmapped.insert(volume);
                }
                self.dirty.insert(volume);
                place.insert(Volume::new())
            }
        }
    }

    // File whether a volume is bound and still open, as it stands now: said
    // wherever its name, its close or its being held changes.
    fn file_open(&mut self, volume: VolumeIndex) {
        let open = self
            .volumes
            .get(&volume)
            .is_some_and(|found| found.name.is_some() && !found.closed);
        match open {
            true => self.open.insert(volume),
            false => self.open.remove(&volume),
        };
    }

    // Something the walk or the layout reads of a volume changed, or the
    // volume came or went: look at it again.
    fn touch(&mut self, volume: VolumeIndex) {
        if let Some(found) = self.volumes.get_mut(&volume) {
            found.invalidate_facts();
        }
        self.dirty.insert(volume);
        self.moved();
    }

    // File what a volume's walk says of the set, as it stands now: whether
    // no volume follows it, and the format its headers were read in. A
    // volume no longer held says nothing.
    fn file(&mut self, volume: VolumeIndex) {
        let walk = self.volumes.get(&volume).map(|found| &found.walk);
        match walk.is_some_and(Walk::is_last) {
            true => self.lasts.insert(volume),
            false => self.lasts.remove(&volume),
        };
        let format = walk
            .and_then(|walk| walk.facts.as_ref())
            .map(|facts| shelf(facts.archive_format()));
        for (at, held) in self.formats.iter_mut().enumerate() {
            match format == Some(at) {
                true => held.insert(volume),
                false => held.remove(&volume),
            };
        }
    }

    // Whether a volume held was read in a format other than `format`.
    fn holds_other_than(&self, format: ArchiveFormat) -> bool {
        let ours = shelf(format);
        self.formats
            .iter()
            .enumerate()
            .any(|(at, held)| at != ours && !held.is_empty())
    }

    // The volumes no piece lies in, from every entry again: where pieces
    // went, at a retraction or a restore.
    fn remap(&mut self) {
        self.pieces_in.clear();
        for (id, entry) in self.entries.iter().enumerate() {
            for (at, piece) in entry.pieces.iter().enumerate() {
                self.pieces_in
                    .entry(piece.volume)
                    .or_default()
                    .push((id, at));
            }
        }
        let mapped: BTreeSet<VolumeIndex> = self.pieces_in.keys().copied().collect();
        self.unmapped = self
            .volumes
            .keys()
            .chain(self.giving.keys())
            .filter(|volume| !mapped.contains(volume))
            .copied()
            .collect();
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    // A range failed its checksum: say so once for each version of its
    // bytes, then wait for a repair to deliver it again.
    fn say_damaged(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        crc: u32,
        out: &mut Vec<Emit>,
    ) {
        if self
            .damage_said
            .insert((volume, range.start, range.end, crc))
        {
            out.push(Emit::Damaged { volume, range });
        }
    }

    // Look again at what changed: the volumes touched, and the files when a
    // key changed. What the store kept in its envelope and its map places
    // elsewhere now is no longer its own, and is said so ahead of any giving
    // up, past `fresh`, a volume range being delivered, which is placed
    // afresh. Only a volume whose headers moved can say so, of the bytes
    // they newly reach.
    fn settle(&mut self, fresh: Option<(VolumeIndex, ByteRange)>, out: &mut Vec<Emit>) {
        if self.gone || (!self.stale && self.dirty.is_empty()) {
            return;
        }
        let mut reached = BTreeMap::new();
        while !self.gone && (std::mem::take(&mut self.stale) | !self.dirty.is_empty()) {
            // The walks, the layout and the keys change only here.
            self.moved();
            let touched = std::mem::take(&mut self.dirty);
            self.walk(&touched, &mut reached, out);
            // The layout holds to every volume touched so far, those the
            // walks gave back or touched again included.
            let touched: BTreeSet<VolumeIndex> = touched.union(&self.dirty).copied().collect();
            if !self.gone {
                self.sync(&touched, out);
            }
            if !self.gone {
                self.open_files(out);
            }
        }
        let mut unkept = Vec::new();
        for (volume, from) in reached {
            self.unkept(volume, from, fresh, &mut unkept);
        }
        let last = self.gone.then(|| out.pop()).flatten();
        out.extend(unkept);
        out.extend(last);
    }

    // Walk the headers of the volumes touched as far as their bytes go,
    // noting for each one whose headers moved how far they reached before.
    // A volume nothing touched reads as it did.
    fn walk(
        &mut self,
        touched: &BTreeSet<VolumeIndex>,
        reached: &mut BTreeMap<VolumeIndex, u64>,
        out: &mut Vec<Emit>,
    ) {
        for &volume in touched {
            let password = self.header_password();
            let Some(found) = self.volumes.get_mut(&volume) else {
                continue;
            };
            #[cfg(test)]
            counts::bump(|counts| {
                counts.walks += 1;
                counts.walked.insert(volume.0);
            });
            let before = found.walk.reach().unwrap_or(0);
            // Password changes can schedule a walk without touching the
            // volume. The walk can change parsed facts or wanted ranges.
            found.invalidate_facts();
            let step = found.walk.step(
                &mut found.kept,
                found.length,
                password.as_deref().map(String::as_str),
                self.keys.kdf(),
                found.closed,
            );
            self.file(volume);
            let kept = &self.volumes[&volume].kept;
            match step {
                Step::Walked { moved, damaged } => {
                    if moved {
                        reached.entry(volume).or_insert(before);
                    }
                    if let Some(range) = damaged {
                        let crc = kept.read(range).map_or(0, crc32);
                        self.say_damaged(volume, range, crc, out);
                    }
                }
                Step::Locked(encryption) => self.open_headers(volume, &encryption, out),
                Step::Broken(range) => {
                    if kept.verified(range) {
                        return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
                    }
                    let crc = kept.read(range).map_or(0, crc32);
                    self.say_damaged(volume, range, crc, out);
                }
                Step::Refuse(emit) => return self.give_up(emit, out),
            }
            if self.gone {
                return;
            }
            self.give_back_misplaced(volume, out);
        }
        self.give_back_past_last(out);
    }

    // A volume read to its close whose own headers say it is not where the
    // store holds it was claimed on too little: its front said nothing that
    // placed it, and a founding put it first. It goes back to its namespace
    // with everything read of it, to be claimed where it belongs. Only at
    // its close, so nothing of it is still on its way here. What travels is
    // the walk as far as it got: a password that came after the close lets
    // it through no further than the main header, and the headers below
    // wait for the next owner to want them.
    // The store never mapped any of it, since the layout stops at a volume
    // its headers do not place. The volume held first is the one the store
    // was founded on, and every other was taken as a volume of the set it
    // begins: when that one proves to be no set's first, the store was
    // never that set, and each volume it took goes back with it, read to its
    // close, for the store that holds their first volume to claim.
    fn give_back_misplaced(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        let misplaced = self
            .volumes
            .get(&volume)
            .is_some_and(|found| found.closed && found.misplaced(volume));
        if !misplaced {
            return;
        }
        let going: Vec<VolumeIndex> = match volume == VolumeIndex(0) {
            true => self
                .volumes
                .iter()
                .filter(|(_, found)| found.closed)
                .map(|(index, _)| *index)
                .collect(),
            false => vec![volume],
        };
        self.give_back_held(going, out);
    }

    // A set ends at the volume whose headers say no volume follows it. A
    // volume held past it was claimed on a number every set states alike:
    // it goes back at its close, as a misplaced one does, for the store
    // whose set it is to claim.
    fn give_back_past_last(&mut self, out: &mut Vec<Emit>) {
        let Some(last) = self.last() else {
            return;
        };
        let going: Vec<VolumeIndex> = self
            .volumes
            .range((Bound::Excluded(last), Bound::Unbounded))
            .filter(|(_, found)| found.closed)
            .map(|(index, _)| *index)
            .collect();
        if !going.is_empty() {
            self.give_back_held(going, out);
        }
    }

    fn give_back_held(&mut self, going: Vec<VolumeIndex>, out: &mut Vec<Emit>) {
        for index in going {
            if let Some(found) = self.volumes.remove(&index) {
                self.giving.insert(index, found);
                out.push(Emit::GiveBack(index));
            }
            self.file(index);
            self.file_open(index);
            self.touch(index);
        }
    }

    // Everything bound has closed: nothing more will arrive.
    fn finished(&self) -> bool {
        self.open.is_empty()
    }

    // The last volume, once some volume's headers say it is.
    fn last(&self) -> Option<VolumeIndex> {
        self.lasts.first().copied()
    }

    // Numbered volumes that never came, once everything else has: damaged
    // whole, at the length every volume but the last shares. A volume held
    // with no name may place bytes, so it is said alone; the runs between
    // the volumes held hold nothing and are each said once, as a gap. The
    // cost is that of the volumes held, never of the last one's number.
    fn say_gaps(&mut self, out: &mut Vec<Emit>) {
        let Some(last) = self.last().filter(|_| self.finished()) else {
            return;
        };
        let Some(length) = self
            .volumes
            .iter()
            .filter(|(index, volume)| **index != last && volume.closed)
            .find_map(|(_, volume)| volume.length)
        else {
            return;
        };
        let range = ByteRange::new(0, length);
        let held: Vec<(u32, bool)> = self
            .volumes
            .range(..last)
            .map(|(index, found)| (index.0, found.name.is_none()))
            .collect();
        let mut next = 0;
        for (index, nameless) in held {
            self.say_gap(next, index, range, out);
            if nameless {
                for _ in self.gaps_said.say(index, index + 1) {
                    out.push(Emit::Damaged {
                        volume: VolumeIndex(index),
                        range,
                    });
                }
            }
            next = index + 1;
        }
        self.say_gap(next, last.0, range, out);
    }

    // The volumes from `first` up to `end`, none of them held, as gaps:
    // only the parts not said before.
    fn say_gap(&mut self, first: u32, end: u32, range: ByteRange, out: &mut Vec<Emit>) {
        for (first, end) in self.gaps_said.say(first, end) {
            out.push(Emit::Gap {
                first: VolumeIndex(first),
                end: VolumeIndex(end),
                range,
            });
        }
    }
}

impl Object for RarStore {
    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn facts(&self) -> weaver_graph::Facts {
        facts::of(self)
    }

    // A file key the cache has no answer for: the derivation is the
    // slowest thing the store does, done away from the loop.
    fn work_due(&self) -> bool {
        !self.gone && !self.derive_due.is_empty()
    }

    fn work(&mut self, out: &mut Vec<Emit>) {
        self.derive_due();
        self.settle(None, out);
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if delivery.bytes.is_empty() {
            return;
        }
        let volume = delivery.volume;
        let range = delivery.range();
        // Absorbing can make the object want more of this very delivery: a
        // header's first bytes name its size.
        loop {
            let found = self.volume(volume);
            let generation = found.kept.generation();
            if let Some(at) = found
                .kept
                .absorb(range, delivery.bytes, delivery.provenance)
            {
                found.walk.changed_from(at);
                self.touch(volume);
            }
            self.settle(Some((volume, range)), out);
            if self.gone {
                // The delivery that made the store give its set up still
                // reaches the members announced before it, so what the
                // store answers for those bytes is where they went. Giving
                // up stays its last word, after which nothing is heard.
                let last = out.pop();
                self.route(
                    volume,
                    range,
                    delivery.bytes,
                    delivery.provenance,
                    read,
                    out,
                );
                out.extend(last);
                return;
            }
            // A damaged delivery over the walk's next bytes is kept whole, so
            // it is said once and its repair is seen.
            let found = self.volume(volume);
            if delivery.provenance.is_damaged() && !found.closed {
                let hole = found.walk.hole(&found.kept, false);
                if range.contains(hole) {
                    let wanted = found.kept.generation();
                    found.kept.want(ByteRange::new(hole, range.end));
                    if found.kept.generation() != wanted {
                        found.invalidate_facts();
                    }
                }
            }
            if found.kept.generation() == generation {
                break;
            }
            // A range wanted anew is the store's own now, not held.
            self.moved();
        }
        self.route(
            volume,
            range,
            delivery.bytes,
            delivery.provenance,
            read,
            out,
        );
        if let Some(candidate) = self.wrong_key.take() {
            self.condemn(candidate, out);
            // The files it keyed are locked again: the next candidate opens
            // them, or the store asks for one.
            self.settle(None, out);
        }
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        // A volume adopted closed was walked to its close already.
        let found = self.volume(volume);
        if !std::mem::replace(&mut found.closed, true) {
            self.file_open(volume);
            self.touch(volume);
        }
        self.settle(None, out);
        if self.gone {
            return;
        }
        self.say_gaps(out);
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        let Some(found) = self.volumes.get(&volume) else {
            return;
        };
        let hole = found.walk.hole(&found.kept, found.closed);
        if found.walk.ended || !range.contains(hole) {
            return;
        }
        // The next header will never come, and nothing past it can be read:
        // the set's own first bytes, when it is the first volume's main
        // header. A loss that runs to the volume's end leaves nothing past
        // it to read.
        let why = if volume == VolumeIndex(0) && found.walk.at_main_header(hole) {
            Why::FirstBytesLost
        } else if found.length.is_some_and(|length| range.end >= length) {
            return;
        } else {
            Why::Unrepairable
        };
        self.give_up(Emit::Refuse(why), out);
    }

    // Nothing more will arrive. A set whose first volume never came has
    // lost its first bytes: no header says what any volume after it holds.
    // Otherwise a member still damaged, a password still asked for, or a
    // member named and never placed leaves the set unread. A repair that
    // rewrote bytes a candidate was condemned over first has every candidate
    // tried again on them, once the repair is done: nothing reads them again
    // while it still writes.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if std::mem::take(&mut self.rewritten) {
            self.acquit(out);
            self.settle(None, out);
            return;
        }
        let first = self.volumes.get(&VolumeIndex(0));
        let why = match first.is_none_or(|found| found.name.is_none()) {
            true => Some(Why::FirstBytesLost),
            false => crate::quiet::damaged(standing)
                .or_else(|| self.shut())
                .or_else(|| crate::quiet::unplaced(standing)),
        };
        if let Some(why) = why {
            self.give_up(Emit::Refuse(why), out);
        }
    }

    fn wants(&self, out: &mut Vec<Seek>) {
        // The first volume not read to its end.
        let reading = (0..)
            .find(|index| {
                self.volumes
                    .get(&VolumeIndex(*index))
                    .is_none_or(|found| !found.walk.ended)
            })
            .unwrap_or(0);
        for distance in 1..=WINDOW {
            let volume = VolumeIndex(reading + distance);
            let Some(found) = self.volumes.get(&volume) else {
                continue;
            };
            let (true, Some(length), false) = (found.name.is_some(), found.length, found.closed)
            else {
                continue;
            };
            out.push(Seek::data(
                volume,
                ByteRange::new(0, length),
                Weight(distance),
            ));
        }
    }

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {
        // The bids only bring volumes forward; one that cannot be brought
        // forward arrives in the normal order, which the store reads anyway.
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(volume, range, out);
    }

    fn placement(&self) -> Option<u64> {
        Some(self.placed.number())
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

    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange) {
        self.unsay(volume, range);
    }

    // A file's plaintext runs, as its facts keep them from version 5 on:
    // only those its bytes verified, each CRC32 the bytes had as they came.
    // A restart folds them for the file's own check rather than read it.
    fn vouched(&self, member: MemberId, out: &mut Vec<(ByteRange, u32)>) {
        let Some(entry) = self.entries.get(member.0 as usize) else {
            return;
        };
        out.extend(
            entry
                .plain_runs
                .kept()
                .into_iter()
                .filter(|&(_, _, _, verified)| verified)
                .map(|(start, len, crc, _)| (ByteRange::at(start, len), crc)),
        );
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        // A length said anew is the producer's word now: the volume is
        // walked again against it.
        let found = self.volume(volume);
        if found.length == Some(length) {
            return;
        }
        found.length = Some(length);
        self.touch(volume);
        self.settle(None, out);
    }

    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        // The offer answers the question the store asked: a candidate that
        // opens nothing leaves it free to ask for the next one.
        self.asked = false;
        self.candidate(secret);
        self.dirty.extend(self.volumes.keys().copied());
        self.stale = true;
        self.settle(None, out);
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes> {
        self.volumes
            .get(&VolumeIndex(envelope))?
            .kept
            .read(range)
            .map(bytes::Bytes::copy_from_slice)
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], _length: Option<u64>) {
        let facts = self.front_facts(front);
        let bound = self.volume(volume);
        bound.name_as(name);
        bound.front = facts;
        self.file_open(volume);
        self.touch(volume);
    }

    fn engine_volume(&self, volume: VolumeIndex) -> Option<weaver_graph::Carried> {
        let held = self.volumes.get(&volume)?;
        Some(Box::new(crate::engine::CarriedVolume {
            front: held.stated().cloned(),
            length: held.length,
            window: held.stated().and_then(crate::rar_engine::window),
        }))
    }

    // The place the volume's headers or name give it, held or not: whether
    // it takes it is the tree's to judge.
    fn claim(&mut self, name: &str, first_bytes: &[u8], _length: Option<u64>) -> Claim {
        self.claim_volume(name, first_bytes)
    }

    // A volume contributes to no member yet and can travel with all the
    // parse it owns. This is also the guard on a transfer the tree asks for.
    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        if !self.unmapped.contains(&volume) {
            return None;
        }
        let held = self
            .giving
            .get(&volume)
            .or_else(|| self.volumes.get(&volume))?;
        (held.kept.len() <= claim::CARRIED).then_some(held)
    }

    fn carriable(&self, out: &mut Vec<VolumeIndex>) {
        out.extend(
            self.unmapped
                .iter()
                .copied()
                .filter(|volume| self.carried(*volume).is_some()),
        );
    }

    fn give_back(&mut self, volume: VolumeIndex) -> Option<weaver_graph::Carried> {
        self.carried(volume)?;
        let given = self
            .giving
            .remove(&volume)
            .or_else(|| self.volumes.remove(&volume));
        if !self.volumes.contains_key(&volume) && !self.giving.contains_key(&volume) {
            self.unmapped.remove(&volume);
        }
        self.file(volume);
        self.file_open(volume);
        self.touch(volume);
        given.map(|volume| Box::new(volume) as weaver_graph::Carried)
    }

    // A volume links alone placed: no layout maps it, so it goes as it is.
    fn provisional(
        &self,
        volume: VolumeIndex,
        name: &str,
        first_bytes: &[u8],
        carried: Option<&weaver_graph::Carried>,
    ) -> bool {
        let Some(holder) = self.volumes.get(&volume) else {
            return false;
        };
        let given = carried.and_then(|carried| carried.downcast_ref::<Volume>());
        self.yields(volume, holder, name, first_bytes, given)
    }

    fn reclaim(
        &self,
        name: &str,
        front: &[u8],
        _length: Option<u64>,
        carried: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        match carried.downcast_ref::<Volume>() {
            Some(given) => self.reclaim_volume(name, front, given),
            None => carried
                .downcast_ref::<crate::engine::CarriedVolume<unrar_rs::RarVolumeFacts>>()
                .map_or_else(Claim::default, |given| {
                    self.reclaim_front(name, front, given.front.as_ref())
                }),
        }
    }

    // The volume as its giver read it: its headers walked, its bytes kept,
    // its length and close. None of its bytes is read again. No password
    // travels with it: its headers are walked again under this store's own,
    // so headers no password of the store opens ask the job, and every
    // password the store holds is one the job offered it.
    fn adopt(
        &mut self,
        volume: VolumeIndex,
        name: &str,
        front: &[u8],
        length: Option<u64>,
        carried: weaver_graph::Carried,
    ) {
        let Ok(mut held) = carried.downcast::<Volume>() else {
            return self.bound(volume, name, front, length);
        };
        held.name_as(name);
        *self.volume(volume) = *held;
        self.file(volume);
        self.file_open(volume);
        self.touch(volume);
    }

    fn renamed(&mut self, volume: VolumeIndex, name: &str, out: &mut Vec<Emit>) {
        let Some(found) = self.volumes.get_mut(&volume) else {
            return;
        };
        found.name_as(name);
        // Finishing a RAR4 walk need not reveal an ordinal. A corrected
        // name then places it more precisely than the links that held it;
        // only a position the headers actually state outranks that name.
        let stated = found.stated().is_some_and(claim::states_position);
        let misplaced = found
            .numbered
            .as_ref()
            .is_some_and(|named| named.index() != volume.0)
            && !stated;
        // The name is what settles a volume its headers do not place, and
        // what it settles is said now, as a length said anew is: a
        // checkpoint before the next delivery records the name, and a
        // restore reads it again as said already.
        self.file_open(volume);
        self.touch(volume);
        if misplaced && self.carried(volume).is_some() {
            self.give_back_held(vec![volume], out);
        }
        self.settle(None, out);
    }

    fn refused(&mut self) {
        self.gone = true;
    }
}
