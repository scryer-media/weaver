// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The engine object, shared by every engine format: conventional
// extraction of a set that sits on disk as files, whole or still growing.
// The object itself keeps almost nothing: the set's names, and what its
// volumes' fronts state of their places, so it can claim the set's other
// volumes, which volumes it has, and a password the job offered. How far
// the run has read is the owner's, which hands it in to bid for the
// volumes the run reaches next.
// Every byte of its volumes is envelope, kept by the owner; the run reads
// them back. A format supplies only how its volumes are named, what their
// fronts say of their places, and what a run of it is.

mod joined;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::OnceLock;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Claim, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, Located,
    MemberId, Object, Placement, Pull, Seek, Standing, VolumeIndex, Weight, Why,
};

use crate::beside::Beside;

pub(crate) use joined::Joined;

// What one engine format knows: its kind, how its set names its volumes,
// what a volume's front states of its place, and the run that extracts it.
pub trait Format: Send + Sync {
    const KIND: Kind;
    type Name: SetName;
    // What a volume's front states of its place in its set, for a set whose
    // names say nothing of it. Serde, because it is an engine's fact.
    type Front: Serialize + DeserializeOwned + Send + Sync;
    fn parse(name: &str) -> Option<Self::Name>;
    // A volume's front, as far as it places the volume: `None` for a format
    // whose volumes state nothing of their places.
    fn front(_first_bytes: &[u8]) -> Option<Self::Front> {
        None
    }
    // Whether the front states the volume's place itself, rather than only
    // linking it to the volumes beside it.
    fn states_place(_front: &Self::Front) -> bool {
        false
    }
    // Whether the front says the volume is not at `index`.
    fn misplaced(_front: &Self::Front, _index: u32) -> bool {
        false
    }
    // Where the volumes bound place a volume with `front`, when its front
    // states one, and `length`, once known: each bound volume by index,
    // with its front and its length.
    fn place(
        _bound: &Holdings<'_, Self::Front>,
        _front: Option<&Self::Front>,
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        None
    }

    // Whether a placement also judges membership across conflicting set
    // names. Formats whose headers only state a position must distinguish
    // that from evidence linking the candidate to the bound volumes.
    fn judged(
        _bound: &Holdings<'_, Self::Front>,
        _front: Option<&Self::Front>,
        _length: Option<u64>,
    ) -> bool {
        true
    }

    // Whether the front held at `index` is tied to a volume bound beside
    // it, the one before or the one after, by what both fronts state: for
    // a RAR set, a member carried on from the one before or into the one
    // after. A front that merely states nothing against its neighbours is
    // not tied to them.
    fn linked(_bound: &Holdings<'_, Self::Front>, _front: &Self::Front, _index: u32) -> bool {
        false
    }

    // Where a volume of `length` states its place when its front does not:
    // the bytes at its end that `told` reads. `None` where nothing past the
    // front states it, or the front cannot say where that is.
    fn tail(_front: &Self::Front, _length: u64) -> Option<ByteRange> {
        None
    }

    // What the bytes of `tail` state of the volume's place, filed into its
    // front. Whatever they say, they are all there is: nothing more is read.
    fn told(_front: &mut Self::Front, _tail: &[u8]) {}
    fn run(password: Option<String>) -> Box<dyn Extraction>;

    // The least a run of this format holds resident, whatever its volumes'
    // fronts say.
    fn reserve() -> u64 {
        0
    }

    // What a decode of the members whose headers lie in a volume's front
    // holds resident, as those headers state it: `None` where the front
    // states nothing of it.
    fn window(_first_bytes: &[u8]) -> Option<u64> {
        None
    }

    // Whether a run's decoder borrows a window over the run's grant from
    // `Budget::lender`: `Engine::lends`.
    fn lends() -> bool {
        false
    }
}

// A volume bound, as far as it places the others: what its front states,
// and its length once known.
pub struct Holding<'a, T> {
    pub front: Option<&'a T>,
    pub length: Option<u64>,
}

// The volumes an engine has bound, but one it is placing again, read by
// index from the facts it keeps as each volume comes and goes: a lookup is
// a lookup, and nothing is copied out to answer one.
pub struct Holdings<'a, T> {
    bound: &'a BTreeSet<u32>,
    fronts: &'a BTreeMap<u32, T>,
    lengths: &'a BTreeMap<u32, u64>,
    beside: &'a Beside,
    except: Option<u32>,
}

impl<'a, T> Holdings<'a, T> {
    pub fn get(&self, index: u32) -> Option<Holding<'a, T>> {
        #[cfg(test)]
        counts::bump(|counts| counts.lookups += 1);
        (Some(index) != self.except && self.bound.contains(&index)).then(|| Holding {
            front: self.fronts.get(&index),
            length: self.lengths.get(&index).copied(),
        })
    }

    // The free places beside a volume held where its front does not say it
    // is not, for a placement that orders a volume by links alone: the
    // places filed beside them, and the one set aside, which is free here.
    // More than the free places beside the others, never fewer.
    pub(crate) fn beside(&self) -> impl Iterator<Item = u32> + 'a {
        let except = self.except.filter(|index| self.beside.holds(*index));
        self.beside.free().chain(except).inspect(|_| {
            #[cfg(test)]
            counts::bump(|counts| counts.walked += 1);
        })
    }

    // Every volume held where its front does not say it is not, in order,
    // but the one set aside: a walk over the set, asked only once no free
    // place takes a volume.
    pub(crate) fn held(&self) -> impl Iterator<Item = u32> + 'a {
        let except = self.except;
        self.beside
            .held()
            .filter(move |index| Some(*index) != except)
            .inspect(|_| {
                #[cfg(test)]
                counts::bump(|counts| counts.walked += 1);
            })
    }
}

// What placing an engine's volumes read: volumes looked up by index, and
// volumes walked over by a placement by links.
#[cfg(test)]
pub(crate) mod counts {
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Counts {
        pub(crate) lookups: usize,
        pub(crate) walked: usize,
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

// Engine volumes hold no parsed output. Only their existing placement
// evidence travels when a corrected name gives one back for reoffering.
pub(crate) struct CarriedVolume<T> {
    pub(crate) front: Option<T>,
    pub(crate) length: Option<u64>,
    // What a decode of the volume holds resident, as the object that parsed
    // it read that from headers the engine's front does not hold.
    pub(crate) window: Option<u64>,
}

// A volume's name as its set numbers it.
// Serde, because the names bound are an engine's facts.
pub trait SetName: Send + Sync + Sized + Serialize + DeserializeOwned {
    // The volume this name is, counted from zero.
    fn index(&self) -> u32;
    fn same_set(&self, other: &Self) -> bool;

    // The volume this name becomes beside the volumes of its set already
    // bound, by index, held or not: a volume posted again is that volume.
    // A name is its own number; a set whose last volume carries none says
    // otherwise.
    fn slot(&self, _bound: &BTreeMap<u32, Self>) -> Option<u32> {
        Some(self.index())
    }

    // The volume that follows this one, where the name alone says which:
    // a set numbered on past its last volume, as RAR's are, names the one
    // a run reads next. `None` where the name may end its set.
    fn after(&self) -> Option<u32> {
        None
    }
}

// The name of volume `volume` of the set `reference` names a volume of, in
// its numbering, width and case, for a set whose names number on past
// their last (`SetName::after`): the volume `Why::Unfound` says a set
// lacks. `None` where the reference names no such volume.
pub fn volume_name(reference: &str, volume: u32) -> Option<String> {
    crate::rar_engine::naming::sibling(reference, volume)
}

// Its facts are the volumes it has, by name and front. Never the password,
// which the owner offers again.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct EngineObject<F: Format> {
    #[serde(skip)]
    format: PhantomData<F>,
    // The names of the volumes bound that name a volume of a set, which
    // every other volume's must share: the set's posted numbering.
    names: BTreeMap<u32, F::Name>,
    // The names a recovery set gave volumes bound in another set's
    // numbering than the posted one. A recovery's name outranks a posted
    // one, so a set its recovery renames is numbered by these from the
    // first such name on, while volumes not renamed yet keep their posted
    // names.
    #[serde(default)]
    recovered: BTreeMap<u32, F::Name>,
    // The first of those names, which says the recovery's numbering is the
    // set's from then on, whichever volumes hold its names: a volume the
    // name moves leaves the object and comes back under it.
    #[serde(default)]
    recovery: Option<F::Name>,
    // What the fronts of the volumes bound state of their places.
    #[serde(default)]
    fronts: BTreeMap<u32, F::Front>,
    // The lengths of the volumes bound, where a format's lengths place
    // its volumes.
    #[serde(default)]
    lengths: BTreeMap<u32, u64>,
    // What a decode of each volume bound holds resident, where its front or
    // the object that handed it on said.
    #[serde(default)]
    windows: BTreeMap<u32, u64>,
    // The volumes held by links alone whose tails may yet state their
    // places (`Format::tail`), until they are read or never will be.
    #[serde(default)]
    tails: BTreeSet<u32>,
    bound: BTreeSet<u32>,
    // The places held where a front does not say its volume is not, and
    // the free places beside them (`Beside`), filed as a volume is bound,
    // its front comes, or it goes. Built from `bound` and `fronts` at the
    // first placement after a restore.
    #[serde(skip)]
    beside: OnceLock<Beside>,
    #[serde(skip)]
    password: Option<String>,
    // Its placement number, which never moves (`Object::placement`).
    #[serde(skip)]
    placed: crate::placed::Placed,
}

impl<F: Format> Durable for EngineObject<F> {
    // 3: a directory is a member, numbered in the walk with the files, so the
    // members recorded under an older run carry other ids. 4: a 7z engine
    // holds one file, its pieces' names and places dropped, since a 7z in
    // numbered pieces is the split store's.
    const VERSION: u16 = 4;

    fn impossible(&self) -> Option<&'static str> {
        let known = self
            .names
            .keys()
            .chain(self.recovered.keys())
            .chain(self.fronts.keys())
            .chain(self.lengths.keys())
            .chain(self.windows.keys())
            .chain(self.tails.iter());
        let named = known.into_iter().all(|volume| self.bound.contains(volume));
        (!named).then_some("a volume named that is not bound")
    }
}

impl<F: Format> Default for EngineObject<F> {
    fn default() -> Self {
        Self {
            format: PhantomData,
            names: BTreeMap::new(),
            recovered: BTreeMap::new(),
            recovery: None,
            fronts: BTreeMap::new(),
            lengths: BTreeMap::new(),
            windows: BTreeMap::new(),
            tails: BTreeSet::new(),
            bound: BTreeSet::new(),
            beside: OnceLock::new(),
            password: None,
            placed: crate::placed::Placed::default(),
        }
    }
}

impl<F: Format> EngineObject<F> {
    pub fn new() -> Self {
        Self::default()
    }

    // A volume's place is what states it, as the RAR store places one: the
    // position its front gives, beside what the fronts of the volumes bound
    // state; else its name in the set's own numbering where the front does
    // not say the volume is not there; and only a volume that places
    // neither by what links it to the volumes bound, its front or its
    // length, which guess wrong across a volume not yet bound. A posted
    // name is a label, which a recovery set may change later, so the front
    // outranks it. A place bound already is answered all the same: the
    // volume posted again is that volume.
    fn placed(&self, name: &str, first_bytes: &[u8], length: Option<u64>) -> Option<VolumeIndex> {
        self.placed_front(name, F::front(first_bytes).as_ref(), length)
    }

    // Each volume bound, but `except`, with its front and its length.
    fn holdings(&self, except: Option<u32>) -> Holdings<'_, F::Front> {
        Holdings {
            bound: &self.bound,
            fronts: &self.fronts,
            lengths: &self.lengths,
            beside: self.beside.get_or_init(|| {
                let mut beside = Beside::default();
                for volume in &self.bound {
                    beside.file(*volume, self.held(*volume));
                }
                beside
            }),
            except,
        }
    }

    // Whether a volume is held at `volume` as it places the others: bound,
    // and not where its front says it is not.
    fn held(&self, volume: u32) -> bool {
        self.bound.contains(&volume)
            && !self
                .fronts
                .get(&volume)
                .is_some_and(|front| F::misplaced(front, volume))
    }

    // File where `volume` stands, as its binding or front changed, once
    // the places are filed at all.
    fn file(&mut self, volume: u32) {
        let held = self.held(volume);
        if let Some(beside) = self.beside.get_mut() {
            beside.file(volume, held);
        }
    }

    // Whether the posted name of the volume held at `volume` numbers it
    // there: what holds it, beside its links.
    fn named_at(&self, volume: u32) -> bool {
        self.names
            .get(&volume)
            .is_some_and(|name| name.slot(&self.names) == Some(volume))
    }

    // Where the volume bound at `volume` states its place, while its links
    // alone hold it there: its front states none, and no name, posted or
    // recovered, numbers it there.
    fn tail(&self, volume: u32) -> Option<ByteRange> {
        let front = self.fronts.get(&volume)?;
        let alone = !F::states_place(front)
            && !self.named_at(volume)
            && !self.recovered.contains_key(&volume);
        F::tail(front, *self.lengths.get(&volume)?).filter(|_| alone)
    }

    // A volume just bound, or whose front just came, waits for its tail
    // where that states its place.
    fn await_tail(&mut self, volume: u32) {
        match self.bound.contains(&volume) && self.tail(volume).is_some() {
            true => self.tails.insert(volume),
            false => self.tails.remove(&volume),
        };
    }

    // The tail of a volume waiting for it, once a clean delivery brings the
    // last of it: the rest read back from where its bytes went. What it
    // states is filed with the volume's front, and a volume it says is not
    // where its links hold it goes back to be claimed where it is.
    fn read_tail(
        &mut self,
        delivery: &Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let volume = delivery.volume.0;
        if !self.tails.contains(&volume) || delivery.provenance.is_damaged() {
            return;
        }
        let Some(tail) = self.tail(volume) else {
            self.tails.remove(&volume);
            return;
        };
        let Some(here) = tail.intersect(delivery.range()) else {
            return;
        };
        let mut bytes = Vec::with_capacity(tail.len() as usize);
        let before = ByteRange::new(tail.start, here.start);
        if !before.is_empty()
            && !read(
                &Placement::Envelope {
                    envelope: volume,
                    range: before,
                },
                &mut bytes,
            )
        {
            return;
        }
        let from = (here.start - delivery.offset) as usize;
        bytes.extend_from_slice(&delivery.bytes[from..from + here.len() as usize]);
        let after = ByteRange::new(here.end, tail.end);
        if !after.is_empty()
            && !read(
                &Placement::Envelope {
                    envelope: volume,
                    range: after,
                },
                &mut bytes,
            )
        {
            return;
        }
        self.tails.remove(&volume);
        let Some(front) = self.fronts.get_mut(&volume) else {
            return;
        };
        F::told(front, &bytes);
        let misplaced = F::misplaced(front, volume);
        self.file(volume);
        if misplaced {
            out.push(Emit::GiveBack(delivery.volume));
        }
    }

    // Whether a claimant's front, the one it carries or the one it opens
    // on, states the place `volume`, beside the volumes bound but the one
    // there.
    fn states(
        &self,
        volume: VolumeIndex,
        first_bytes: &[u8],
        carried: Option<&CarriedVolume<F::Front>>,
    ) -> bool {
        let read;
        let front = match carried.and_then(|carried| carried.front.as_ref()) {
            Some(front) => Some(front),
            None => {
                read = F::front(first_bytes);
                read.as_ref()
            }
        };
        let length = carried.and_then(|carried| carried.length);
        front.is_some_and(|front| {
            F::states_place(front)
                && F::place(&self.holdings(Some(volume.0)), Some(front), length) == Some(volume)
        })
    }

    // Which of the set's numberings a name is in: the one its recovery set
    // gave, which outranks the posted one, or the posted one.
    fn numbering(&self, candidate: &F::Name) -> Numbering {
        let recovery = self.recovery.as_ref();
        let posted = self.names.values().next();
        match (
            recovery.map(|first| first.same_set(candidate)),
            posted.map(|first| first.same_set(candidate)),
        ) {
            (Some(true), _) => Numbering::Recovered,
            (_, Some(true)) => Numbering::Posted,
            (None, None) => Numbering::Unanchored,
            _ => Numbering::Elsewhere,
        }
    }

    // Whether the volume's front ties it to the volumes bound, whatever its
    // name says: to a volume bound beside it, in two lookups. Where neither
    // place beside it is bound yet, nothing there says either way, and it
    // is tied where its links place it among the others, a front that does
    // not state its place: that placement may walk the set, once per such
    // rename. A volume bound beside it that its front does not link to says
    // it is not this set's.
    fn held_here(&self, volume: VolumeIndex) -> bool {
        let Some(front) = self.fronts.get(&volume.0) else {
            return false;
        };
        let others = self.holdings(Some(volume.0));
        let alone = [volume.0.checked_sub(1), volume.0.checked_add(1)]
            .into_iter()
            .flatten()
            .all(|beside| others.get(beside).is_none());
        F::linked(&others, front, volume.0)
            || (alone
                && !F::states_place(front)
                && F::place(&others, Some(front), self.lengths.get(&volume.0).copied()).is_some())
    }

    fn names_in(&self, numbering: Numbering) -> &BTreeMap<u32, F::Name> {
        match numbering {
            Numbering::Recovered => &self.recovered,
            _ => &self.names,
        }
    }

    fn placed_front(
        &self,
        name: &str,
        front: Option<&F::Front>,
        length: Option<u64>,
    ) -> Option<VolumeIndex> {
        let bound = self.holdings(None);
        let stated = front.is_some_and(F::states_place);
        let wanted = F::parse(name);
        let numbering = wanted.as_ref().map(|candidate| self.numbering(candidate));
        let elsewhere = numbering == Some(Numbering::Elsewhere);
        // Where links place it, asked only where the answer is used: a
        // placement by links may walk the set.
        let linked = std::cell::OnceCell::new();
        let placed = || {
            *linked.get_or_init(|| {
                F::place(&bound, front, length)
                    .filter(|_| !elsewhere || F::judged(&bound, front, length))
            })
        };
        if stated && let Some(placed) = placed() {
            return Some(placed);
        }
        let named = wanted
            .zip(numbering)
            .filter(|(_, numbering)| match numbering {
                // With no naming anchor, a name only refines a place the
                // format already establishes; it cannot introduce
                // unrelated siblings of a lone unnumbered archive.
                Numbering::Unanchored => placed().is_some(),
                Numbering::Elsewhere => false,
                Numbering::Posted | Numbering::Recovered => true,
            })
            .and_then(|(candidate, numbering)| {
                let slot = candidate.slot(self.names_in(numbering))?;
                // A place a recovery's name holds is not a posted name's.
                (numbering != Numbering::Posted || !self.recovered.contains_key(&slot))
                    .then_some(slot)
            })
            .filter(|index| !front.is_some_and(|front| F::misplaced(front, *index)));
        named.map(VolumeIndex).or_else(placed)
    }

    // The volume the set lacks, once nothing more reaches its run: with
    // its volumes bound from the first and none between them missing, a
    // member its run never placed, nothing lost, ran on past the last,
    // where a run finds no volume and ends silently (the RAR run's
    // `Input::get_volume`). The last volume's name says which follows it,
    // where the set's names number on past their last.
    fn lacks(&self) -> Option<u32> {
        let last = self.bound.last()?;
        let from_first = self.bound.len() as u64 == u64::from(*last) + 1;
        self.recovered
            .get(last)
            .or_else(|| self.names.get(last))?
            .after()
            .filter(|_| from_first)
    }
}

// Which of a set's numberings a name is in (`EngineObject::numbering`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Numbering {
    // The set has no name yet.
    Unanchored,
    Posted,
    Recovered,
    // Neither: the name is another set's.
    Elsewhere,
}

impl<F: Format + 'static> Object for EngineObject<F> {
    fn kind(&self) -> Kind {
        F::KIND
    }

    // The run reads the volumes from the owner's files; the bytes on their
    // way there say nothing the run does not read itself, but a volume's
    // front. A volume can be bound before its front comes, as a restored
    // volume fetched again is: what its front states of its place is the
    // engine's when it does. So is what a volume's tail states of it, for
    // one its links alone hold (`read_tail`).
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let volume = delivery.volume.0;
        if !self.bound.contains(&volume) {
            return;
        }
        if delivery.offset == 0 {
            if let Some(front) = F::front(delivery.bytes)
                && !self.fronts.contains_key(&volume)
            {
                self.fronts.insert(volume, front);
                self.file(volume);
                self.await_tail(volume);
            }
            if let Some(window) = F::window(delivery.bytes) {
                self.windows.entry(volume).or_insert(window);
            }
        }
        self.read_tail(&delivery, read, out);
    }

    // A closed volume was handed every byte it has, its tail with them: one
    // whose tail stated nothing by then, or that a restore closes again
    // without them, stays where its links hold it.
    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.tails.remove(&volume.0);
    }

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // Nothing more will reach the run. A member it left damaged, a password
    // it is still asking for, a member it named and never closed: the set
    // cannot be read whole, and its volumes go back as they stand. A set
    // below the root that is alone in its namespace, its volumes short of
    // a run from the first or a member it named still open, has volumes
    // elsewhere: it gives its volumes up to be lifted (`Why::Scattered`).
    // A set short of the volume after its last says which it lacks
    // (`Why::Unfound`).
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        let why = crate::quiet::damaged(standing)
            .or_else(|| standing.asking().then_some(Why::EncryptedHeader))
            // My run found bad what proof above found whole: I cannot read
            // my volumes.
            .or_else(|| (standing.refuted() && !standing.lost()).then_some(Why::InvalidArchive))
            .or_else(|| {
                let short = self
                    .bound
                    .iter()
                    .enumerate()
                    .any(|(at, volume)| at as u64 != u64::from(*volume));
                let unplaced = crate::quiet::unplaced(standing, self.lacks());
                // A member open for bytes lost is short of them, not of
                // volumes.
                let open = unplaced.is_some() && !standing.lost();
                match standing.liftable() && (open || short) {
                    true => Some(Why::Scattered),
                    false => unplaced,
                }
            });
        out.extend(why.map(Emit::Refuse));
    }

    // What it bids for of itself is the tails of the first volumes its links
    // alone hold, as many as the RAR store bids ahead; the rest of what it
    // bids for follows its run.
    fn wants(&self, out: &mut Vec<Seek>) {
        let ahead = self.tails.iter().take(crate::rar_store::WINDOW as usize);
        for (weight, &volume) in (0..).zip(ahead) {
            if let Some(tail) = self.tail(volume) {
                out.push(Seek::data(VolumeIndex(volume), tail, Weight(weight)));
            }
        }
    }

    // A tail nothing will bring, or that comes only in the normal order,
    // states nothing ahead of it: the volume stays where its links hold it.
    fn pulled(&mut self, seek: Seek, pull: Pull, _out: &mut Vec<Emit>) {
        let volume = seek.volume.0;
        if matches!(pull, Pull::Lost | Pull::Unseekable) && self.tail(volume) == Some(seek.range) {
            self.tails.remove(&volume);
        }
    }

    // The fronts it copies out are in its facts, which a restart keeps, and
    // its run reads the volumes themselves: it says nothing of the posted bytes.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

    // Every range is its envelope, whatever arrived: `locate` reads
    // nothing, so its number never moves.
    fn placement(&self) -> Option<u64> {
        Some(self.placed.number())
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

    fn offer_password(&mut self, secret: &str, _out: &mut Vec<Emit>) {
        self.password = Some(secret.to_owned());
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], length: Option<u64>) {
        if let Some(parsed) = F::parse(name) {
            match self.numbering(&parsed) {
                Numbering::Recovered => {
                    self.recovered.insert(volume.0, parsed);
                }
                Numbering::Posted | Numbering::Unanchored => {
                    self.names.insert(volume.0, parsed);
                }
                Numbering::Elsewhere => {}
            }
        }
        if let Some(window) = F::window(front) {
            self.windows.insert(volume.0, window);
        }
        if let Some(front) = F::front(front) {
            self.fronts.insert(volume.0, front);
        }
        if let Some(length) = length {
            self.lengths.insert(volume.0, length);
        }
        self.bound.insert(volume.0);
        self.file(volume.0);
        self.await_tail(volume.0);
    }

    // The name a recovery set gave the volume is the set's numbering now.
    // A recovery set names a set's volumes one at a time, so a name of
    // another set than the posted names is weighed against the names the
    // recovery gave the others, never against posted names it has not
    // replaced yet: the first such name starts the recovery's numbering
    // where the volume's front holds it in this set by the volumes beside
    // it, as a nameless RAR4 front always does. A name of another set than
    // the recovery's numbering, or one no front holds here, is elsewhere.
    fn renamed(&mut self, volume: VolumeIndex, name: &str, out: &mut Vec<Emit>) {
        self.names.remove(&volume.0);
        self.recovered.remove(&volume.0);
        let parsed = F::parse(name);
        let numbering = parsed.as_ref().map(|parsed| match self.numbering(parsed) {
            Numbering::Elsewhere if self.recovery.is_none() && self.held_here(volume) => {
                Numbering::Recovered
            }
            numbering => numbering,
        });
        let elsewhere = numbering == Some(Numbering::Elsewhere);
        match (parsed, numbering) {
            (Some(parsed), Some(Numbering::Recovered)) => {
                if self.recovery.is_none() {
                    self.recovery = F::parse(name);
                }
                self.recovered.insert(volume.0, parsed);
            }
            (Some(parsed), Some(Numbering::Posted | Numbering::Unanchored)) => {
                self.names.insert(volume.0, parsed);
            }
            _ => {}
        }
        let corrected = self.placed_front(
            name,
            self.fronts.get(&volume.0),
            self.lengths.get(&volume.0).copied(),
        );
        if corrected.is_some_and(|at| at != volume) || (elsewhere && corrected.is_none()) {
            out.push(Emit::GiveBack(volume));
        }
        // A name that numbers the volume states its place: its tail has
        // nothing left to state, and the volume waits for it no more.
        if self.tail(volume.0).is_none() {
            self.tails.remove(&volume.0);
        }
    }

    fn give_back(&mut self, volume: VolumeIndex) -> Option<weaver_graph::Carried> {
        let given = self.bound.remove(&volume.0).then(|| {
            self.tails.remove(&volume.0);
            self.names.remove(&volume.0);
            self.recovered.remove(&volume.0);
            Box::new(CarriedVolume {
                front: self.fronts.remove(&volume.0),
                length: self.lengths.remove(&volume.0),
                window: self.windows.remove(&volume.0),
            }) as weaver_graph::Carried
        });
        self.file(volume.0);
        given
    }

    fn reclaim(
        &self,
        name: &str,
        front: &[u8],
        length: Option<u64>,
        carried: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        // A volume another kind of object parsed carries nothing the engine
        // reads: it is claimed by its name and front, as a new one is.
        let Some(carried) = carried.downcast_ref::<CarriedVolume<F::Front>>() else {
            return self.placed(name, front, length).into_iter().collect();
        };
        let parsed = F::front(front);
        self.placed_front(
            name,
            carried.front.as_ref().or(parsed.as_ref()),
            carried.length.or(length),
        )
        .into_iter()
        .collect()
    }

    fn adopt(
        &mut self,
        volume: VolumeIndex,
        name: &str,
        front: &[u8],
        length: Option<u64>,
        carried: weaver_graph::Carried,
    ) {
        let Ok(carried) = carried.downcast::<CarriedVolume<F::Front>>() else {
            return self.bound(volume, name, front, length);
        };
        self.bound(volume, name, front, carried.length.or(length));
        if let Some(front) = carried.front {
            self.fronts.insert(volume.0, front);
            self.file(volume.0);
            self.await_tail(volume.0);
        }
        if let Some(window) = carried.window {
            self.windows.insert(volume.0, window);
        }
    }

    // A volume held by its posted name or its links alone, where the member
    // claiming its place holds it by the recovery's numbering, which
    // outranks both. A front that states the holder's place outranks names.
    // One held by its links alone, as a set's last volume handed on from a
    // store before its end record was read sits beside the first, also
    // yields to a claimant whose front states the place, as the RAR store's
    // holder does (`RarStore::yields`): the holder states no place, so it
    // never takes one back from a claimant that does.
    fn provisional(
        &self,
        volume: VolumeIndex,
        name: &str,
        first_bytes: &[u8],
        carried: Option<&weaver_graph::Carried>,
    ) -> bool {
        if !self.bound.contains(&volume.0)
            || self.recovered.contains_key(&volume.0)
            || self.fronts.get(&volume.0).is_some_and(F::states_place)
        {
            return false;
        }
        let recovered = F::parse(name).is_some_and(|claimant| {
            self.numbering(&claimant) == Numbering::Recovered
                && claimant.slot(&self.recovered) == Some(volume.0)
        });
        let carried = carried.and_then(|carried| carried.downcast_ref::<CarriedVolume<F::Front>>());
        recovered || (!self.named_at(volume.0) && self.states(volume, first_bytes, carried))
    }

    // The place the volume's facts give it, held or not: whether it takes
    // it is the tree's to judge.
    fn claim(&mut self, name: &str, clean: weaver_graph::Clean<'_>, length: Option<u64>) -> Claim {
        let first_bytes = clean.front();
        self.placed(name, first_bytes, length).into_iter().collect()
    }

    fn reads_in_order(&self) -> bool {
        true
    }

    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }

    fn facts(&self) -> Facts {
        Facts::of(F::KIND, self)
    }
}

// What a run of format `F` reserves: the widest decode its volumes state,
// and the format's least. A store that runs a format's decode over part of
// a set reserves by the same rule.
pub(crate) fn reserve<F: Format>(windows: impl IntoIterator<Item = u64>) -> Option<u64> {
    let widest = windows.into_iter().max().unwrap_or(0);
    Some(F::reserve().max(widest))
}

impl<F: Format + 'static> Engine for EngineObject<F> {
    fn start(&mut self) -> Box<dyn Extraction> {
        F::run(self.password.clone())
    }

    // The widest decode any volume bound states, and the format's least.
    // A volume whose front states nothing adds nothing: what its members
    // hold cannot be known before the run reads their headers.
    fn reserve(&self) -> Option<u64> {
        reserve::<F>(self.windows.values().copied())
    }

    fn lends(&self) -> bool {
        F::lends()
    }

    // A volume its links alone hold is not placed while its tail may yet
    // say it belongs elsewhere.
    fn placed(&self, volume: VolumeIndex) -> bool {
        !self.tails.contains(&volume.0)
    }

    // The volumes after the one the run is on, the nearest first: weighed
    // by how many bound volumes lie between, since an index orders the
    // volumes and need not be dense.
    fn bid(&self, reached: VolumeIndex, out: &mut Vec<Seek>) {
        let after = (Bound::Excluded(reached.0), Bound::Unbounded);
        for (distance, &volume) in (1..).zip(self.bound.range(after)) {
            out.push(Seek::data(
                VolumeIndex(volume),
                ByteRange::new(0, u64::MAX),
                Weight(distance),
            ));
        }
    }
}
